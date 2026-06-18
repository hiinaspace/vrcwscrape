#!/usr/bin/env python
"""R1 stage 1 — macro hierarchy layer driver + render checkpoint.

Generates a tiered arterial road network (highways/majors) and macro-blocks
from the staged island inputs, by extending Arm B. See
``docs/large-scale-growth-research.md`` (staged build plan, step 1) and
``mapgen/src/mapgen/r1_macro.py``.

Outputs to mapgen/artifacts/r1/macro/ (or --out-dir):
  macro_overview.png   — density backdrop + macro-blocks + tiered arterials +
                         nodes + (light) world points
  arterials.geojson    — tiered arterial LineStrings (tier/tau/length props)
  macro_blocks.geojson — macro-block Polygons
  macro_nodes.geojson  — city/town nodes
  macro_manifest.json  — counts + runtime seconds + params

Run from mapgen/::
    uv run python scripts/run_r1_macro.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.patches as mpatches  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import polars as pl  # noqa: E402
import shapely  # noqa: E402
import shapely.geometry as sg  # noqa: E402

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "src") not in sys.path:
    sys.path.insert(0, str(_REPO / "src"))

from mapgen.r1_arm_a import IslandFields  # noqa: E402
from mapgen.r1_compare import (  # noqa: E402
    _render_boundary,
    _render_density_backdrop,
    _set_extent,
)
from mapgen.r1_macro import (  # noqa: E402
    DEFAULT_BETA_RATIO,
    DEFAULT_COST_FLOOR,
    DEFAULT_MERGE_RADIUS,
    DEFAULT_MIN_BLOCK_AREA,
    DEFAULT_N_CITIES,
    DEFAULT_PEAK_MIN_DISTANCE_UNITS,
    DEFAULT_PEAK_THRESHOLD_FRAC,
    DEFAULT_SEED,
    DEFAULT_SIMPLIFY_TOLERANCE,
    DEFAULT_VILLAGE_MERGE_RADIUS,
    DEFAULT_VILLAGE_PEAK_MIN_DISTANCE_UNITS,
    DEFAULT_VILLAGE_PEAK_THRESHOLD_FRAC,
    DEFAULT_W_DENSITY,
    MacroEdge,
    MacroNode,
    arterials_to_geojson,
    build_density_cost_field,
    build_macro_nodes,
    build_macro_nodes_hierarchical,
    compute_macro_arterials,
    macro_blocks_to_geojson,
    nodes_to_geojson,
    polygonize_macro_blocks,
)
from mapgen.r1_zoom import load_points_with_labels  # noqa: E402

_DEFAULT_IN = _REPO / "artifacts/r1/inputs"
_DEFAULT_OUT = _REPO / "artifacts/r1/macro"

# Tier styling: highway = thick red, major = medium blue, local = thin teal.
_TIER_STYLE: dict[int, dict[str, Any]] = {
    2: {"color": "#b30000", "lw": 3.2, "label": "Highway (city–city)"},
    1: {"color": "#1f5fb0", "lw": 1.7, "label": "Major (≥ town)"},
    0: {"color": "#138d75", "lw": 0.9, "label": "Local (incl. village)"},
}


def load_boundary(inputs_dir: Path) -> sg.Polygon:
    """Load the island boundary Polygon, handling FC/Feature/geometry forms."""
    raw = json.loads((inputs_dir / "island_boundary.geojson").read_text())
    if raw.get("type") == "FeatureCollection":
        geometry = raw["features"][0]["geometry"]
    elif raw.get("type") == "Feature":
        geometry = raw["geometry"]
    else:
        geometry = raw
    geom = sg.shape(geometry)
    if not isinstance(geom, sg.Polygon):
        raise ValueError(f"island boundary must be a Polygon, got {geom.geom_type}")
    return geom


def _boundary_mask(
    boundary: sg.Polygon,
    x0: float,
    y0: float,
    cell: float,
    nrows: int,
    ncols: int,
) -> np.ndarray:
    """Inside-island boolean mask from boundary + cell-centre meshgrid."""
    cols = np.arange(ncols)
    rows = np.arange(nrows)
    xc = x0 + (cols + 0.5) * cell
    yc = y0 + (rows + 0.5) * cell
    xx, yy = np.meshgrid(xc, yc)
    inside = shapely.contains_xy(boundary, xx.ravel(), yy.ravel())
    return inside.reshape(nrows, ncols)


def render_macro_overview(
    *,
    density: np.ndarray,
    height_carved: np.ndarray,
    boundary: sg.Polygon,
    nodes: list[MacroNode],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    macro_blocks: list[sg.Polygon],
    points_path: Path,
    x0: float,
    y0: float,
    cell: float,
    out_path: Path,
    dpi: int = 220,
    draw_points: bool = True,
) -> None:
    """Render the macro checkpoint overview PNG."""
    nrows, ncols = density.shape
    fig, ax = plt.subplots(figsize=(13, 9))

    _render_density_backdrop(ax, density, height_carved, x0, y0, cell, nrows, ncols)
    _render_boundary(ax, boundary)

    # World points: small, low-alpha, colored by l0 cluster (optional, light).
    if draw_points and points_path.exists():
        lp = load_points_with_labels(str(points_path))
        unique = sorted({int(v) for v in lp.l0_ids})
        cmap = plt.get_cmap("tab20")
        color_lookup = {uid: cmap(i % 20) for i, uid in enumerate(unique)}
        pt_colors = [color_lookup[int(i)] for i in lp.l0_ids]
        ax.scatter(
            lp.xs, lp.ys, s=1.5, c=pt_colors, alpha=0.18, linewidths=0.0, zorder=2
        )

    # Macro-block polygon edges (thin gray).
    for poly in macro_blocks:
        if poly.is_empty:
            continue
        rings = [poly.exterior, *list(poly.interiors)]
        for ring in rings:
            rx, ry = ring.xy
            ax.plot(rx, ry, color="#777777", lw=0.7, alpha=0.85, zorder=3)

    # Arterials colored & sized by tier (locals first, highways last / on top).
    for tier in (0, 1, 2):
        if tier not in _TIER_STYLE:
            continue
        style = _TIER_STYLE[tier]
        for line, rec in zip(arterial_lines, edges, strict=True):
            if rec.tier != tier:
                continue
            if line.geom_type == "LineString":
                lx, ly = line.xy
                ax.plot(
                    lx,
                    ly,
                    color=style["color"],
                    lw=style["lw"],
                    zorder=5 + tier,
                    solid_capstyle="round",
                )

    # Macro nodes by kind: city = large star, town = medium dot, village = small.
    village_x = [nd.x for nd in nodes if nd.kind == "village"]
    village_y = [nd.y for nd in nodes if nd.kind == "village"]
    town_x = [nd.x for nd in nodes if nd.kind == "town"]
    town_y = [nd.y for nd in nodes if nd.kind == "town"]
    city_x = [nd.x for nd in nodes if nd.kind == "city"]
    city_y = [nd.y for nd in nodes if nd.kind == "city"]
    if village_x:
        ax.scatter(
            village_x,
            village_y,
            s=8,
            marker="o",
            c="#555555",
            edgecolors="white",
            linewidths=0.3,
            zorder=7,
        )
    if town_x:
        ax.scatter(
            town_x,
            town_y,
            s=40,
            marker="o",
            c="#222222",
            edgecolors="white",
            linewidths=0.5,
            zorder=8,
        )
    if city_x:
        ax.scatter(
            city_x,
            city_y,
            s=220,
            marker="*",
            c="#ffd000",
            edgecolors="black",
            linewidths=0.8,
            zorder=9,
        )

    _set_extent(ax, boundary, pad=2.0)

    n_city = len(city_x)
    n_town = len(town_x)
    n_village = len(village_x)
    n_hwy = sum(1 for e in edges if e.tier == 2)
    n_major = sum(1 for e in edges if e.tier == 1)
    n_local = sum(1 for e in edges if e.tier == 0)
    ax.set_title(
        "R1 macro hierarchy — 3-tier semantic nodes + tiered arterials\n"
        f"({n_city} cities, {n_town} towns, {n_village} villages, "
        f"{n_hwy} highways, {n_major} majors, {n_local} locals, "
        f"{len(macro_blocks)} macro-blocks)",
        fontsize=11,
    )

    legend_handles = [
        mpatches.Patch(color=_TIER_STYLE[2]["color"], label=_TIER_STYLE[2]["label"]),
        mpatches.Patch(color=_TIER_STYLE[1]["color"], label=_TIER_STYLE[1]["label"]),
        mpatches.Patch(color=_TIER_STYLE[0]["color"], label=_TIER_STYLE[0]["label"]),
        plt.Line2D(
            [0],
            [0],
            marker="*",
            color="w",
            markerfacecolor="#ffd000",
            markeredgecolor="black",
            markersize=14,
            label="City (σ=2, L1 centroid)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor="#222222",
            markersize=8,
            label="Town (σ=1, L0 centroid)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor="#555555",
            markersize=5,
            label="Village (σ=0, density peak)",
        ),
        plt.Line2D([0], [0], color="#777777", lw=0.9, label="Macro-block edge"),
    ]
    ax.legend(handles=legend_handles, loc="upper left", fontsize=8, framealpha=0.9)

    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def run_macro(
    in_dir: Path,
    out_dir: Path,
    *,
    hierarchical: bool = True,
    n_cities: int = DEFAULT_N_CITIES,
    merge_radius: float = DEFAULT_MERGE_RADIUS,
    peak_min_distance_units: float = DEFAULT_PEAK_MIN_DISTANCE_UNITS,
    peak_threshold_frac: float = DEFAULT_PEAK_THRESHOLD_FRAC,
    village_merge_radius: float = DEFAULT_VILLAGE_MERGE_RADIUS,
    village_peak_min_distance_units: float = DEFAULT_VILLAGE_PEAK_MIN_DISTANCE_UNITS,
    village_peak_threshold_frac: float = DEFAULT_VILLAGE_PEAK_THRESHOLD_FRAC,
    cost_base: float = 1.0,
    w_slope: float = 8.0,
    w_river: float = 6.0,
    w_density: float = DEFAULT_W_DENSITY,
    cost_floor: float = DEFAULT_COST_FLOOR,
    beta_ratio: float = DEFAULT_BETA_RATIO,
    simplify_tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE,
    min_block_area: float = DEFAULT_MIN_BLOCK_AREA,
    seed: int = DEFAULT_SEED,
    draw_points: bool = True,
) -> dict[str, Any]:
    """Run the full macro hierarchy pipeline; return manifest dict."""
    t_start = time.perf_counter()
    np.random.seed(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading inputs…")
    fields = IslandFields.from_npz(str(in_dir / "fields.npz"))
    density = fields.density
    height_carved = fields.height_carved
    flow_accum = fields.flow_accum
    slope = fields.slope
    x0, y0, cell = fields.x0, fields.y0, fields.cell
    nrows, ncols = density.shape

    boundary = load_boundary(in_dir)
    points = pl.read_parquet(in_dir / "island_points.parquet")

    print("Building island mask…")
    mask = _boundary_mask(boundary, x0, y0, cell, nrows, ncols)

    if hierarchical:
        print("Building macro nodes (3-tier semantic: L1 city / L0 town / peak)…")
        nodes = build_macro_nodes_hierarchical(
            density,
            x0,
            y0,
            cell,
            points,
            merge_radius=village_merge_radius,
            peak_min_distance_units=village_peak_min_distance_units,
            peak_threshold_frac=village_peak_threshold_frac,
        )
    else:
        print("Building macro nodes (legacy graded-L0)…")
        nodes = build_macro_nodes(
            density,
            x0,
            y0,
            cell,
            points,
            n_cities=n_cities,
            merge_radius=merge_radius,
            peak_min_distance_units=peak_min_distance_units,
            peak_threshold_frac=peak_threshold_frac,
        )
    n_city = sum(1 for nd in nodes if nd.kind == "city")
    n_town = sum(1 for nd in nodes if nd.kind == "town")
    n_village = sum(1 for nd in nodes if nd.kind == "village")
    print(
        f"  {n_city} cities, {n_town} towns, {n_village} villages ({len(nodes)} nodes)"
    )

    print("Building density-attracting cost field…")
    cost = build_density_cost_field(
        slope,
        flow_accum,
        density,
        mask,
        base=cost_base,
        w_slope=w_slope,
        w_river=w_river,
        w_density=w_density,
        cost_floor=cost_floor,
    )

    print("Computing tiered arterial network…")
    arterial_lines, edges = compute_macro_arterials(
        nodes,
        cost,
        x0,
        y0,
        cell,
        beta_ratio=beta_ratio,
        simplify_tolerance=simplify_tolerance,
    )
    n_hwy = sum(1 for e in edges if e.tier == 2)
    n_major = sum(1 for e in edges if e.tier == 1)
    n_local = sum(1 for e in edges if e.tier == 0)
    print(f"  {len(edges)} edges: {n_hwy} highways, {n_major} majors, {n_local} locals")

    print("Polygonizing macro-blocks…")
    macro_blocks = polygonize_macro_blocks(
        arterial_lines, boundary, min_block_area=min_block_area
    )
    print(f"  {len(macro_blocks)} macro-blocks")

    print("Writing GeoJSON…")
    with (out_dir / "macro_nodes.geojson").open("w") as f:
        json.dump(nodes_to_geojson(nodes), f, indent=2)
    with (out_dir / "arterials.geojson").open("w") as f:
        json.dump(arterials_to_geojson(arterial_lines, edges), f, indent=2)
    with (out_dir / "macro_blocks.geojson").open("w") as f:
        json.dump(macro_blocks_to_geojson(macro_blocks), f, indent=2)

    print("Rendering macro_overview.png…")
    render_macro_overview(
        density=density,
        height_carved=height_carved,
        boundary=boundary,
        nodes=nodes,
        arterial_lines=arterial_lines,
        edges=edges,
        macro_blocks=macro_blocks,
        points_path=in_dir / "island_points.parquet",
        x0=x0,
        y0=y0,
        cell=cell,
        out_path=out_dir / "macro_overview.png",
        draw_points=draw_points,
    )

    runtime_s = round(time.perf_counter() - t_start, 3)

    block_areas = [float(b.area) for b in macro_blocks]
    manifest: dict[str, Any] = {
        "stage": "1 (macro hierarchy layer)",
        "description": "Galin 2011 + Arm B tiered arterials + macro-blocks",
        "seed": seed,
        "seeding": "hierarchical (L1 city / L0 town / peak village)"
        if hierarchical
        else "legacy graded-L0",
        "params": {
            "hierarchical": hierarchical,
            "n_cities": n_cities,
            "merge_radius": merge_radius,
            "peak_min_distance_units": peak_min_distance_units,
            "peak_threshold_frac": peak_threshold_frac,
            "village_merge_radius": village_merge_radius,
            "village_peak_min_distance_units": village_peak_min_distance_units,
            "village_peak_threshold_frac": village_peak_threshold_frac,
            "cost_base": cost_base,
            "w_slope": w_slope,
            "w_river": w_river,
            "w_density": w_density,
            "cost_floor": cost_floor,
            "beta_ratio": beta_ratio,
            "simplify_tolerance": simplify_tolerance,
            "min_block_area": min_block_area,
        },
        "cost_field_note": (
            "cost = (base + w_slope*norm_slope + w_river*(flow>p99)) "
            "- w_density*norm_density; clamped to cost_floor inside mask, "
            "inf outside"
        ),
        "counts": {
            "n_cities": n_city,
            "n_towns": n_town,
            "n_villages": n_village,
            "n_highway_edges": n_hwy,
            "n_major_edges": n_major,
            "n_local_edges": n_local,
            "n_macro_blocks": len(macro_blocks),
        },
        "macro_block_area_stats": (
            {
                "min": round(min(block_areas), 3),
                "max": round(max(block_areas), 3),
                "mean": round(float(np.mean(block_areas)), 3),
                "median": round(float(np.median(block_areas)), 3),
            }
            if block_areas
            else {}
        ),
        "runtime_seconds": runtime_s,
        "outputs": [
            "macro_nodes.geojson",
            "arterials.geojson",
            "macro_blocks.geojson",
            "macro_overview.png",
            "macro_manifest.json",
        ],
    }
    with (out_dir / "macro_manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote macro_manifest.json (runtime {runtime_s}s)")
    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R1 stage 1: macro hierarchy layer (tiered arterials + blocks)"
    )
    parser.add_argument("--in-dir", type=Path, default=_DEFAULT_IN)
    parser.add_argument("--out-dir", type=Path, default=_DEFAULT_OUT)
    parser.add_argument(
        "--legacy-seeding",
        action="store_true",
        help="Use legacy graded-L0 seeding instead of the 3-tier semantic "
        "hierarchy (default: hierarchical)",
    )
    parser.add_argument("--n-cities", type=int, default=DEFAULT_N_CITIES)
    parser.add_argument("--merge-radius", type=float, default=DEFAULT_MERGE_RADIUS)
    parser.add_argument(
        "--peak-min-distance", type=float, default=DEFAULT_PEAK_MIN_DISTANCE_UNITS
    )
    parser.add_argument(
        "--peak-threshold-frac", type=float, default=DEFAULT_PEAK_THRESHOLD_FRAC
    )
    parser.add_argument("--cost-base", type=float, default=1.0)
    parser.add_argument("--w-slope", type=float, default=8.0)
    parser.add_argument("--w-river", type=float, default=6.0)
    parser.add_argument("--w-density", type=float, default=DEFAULT_W_DENSITY)
    parser.add_argument("--cost-floor", type=float, default=DEFAULT_COST_FLOOR)
    parser.add_argument("--beta-ratio", type=float, default=DEFAULT_BETA_RATIO)
    parser.add_argument(
        "--simplify-tolerance", type=float, default=DEFAULT_SIMPLIFY_TOLERANCE
    )
    parser.add_argument(
        "--village-merge-radius", type=float, default=DEFAULT_VILLAGE_MERGE_RADIUS
    )
    parser.add_argument(
        "--village-peak-min-distance",
        type=float,
        default=DEFAULT_VILLAGE_PEAK_MIN_DISTANCE_UNITS,
    )
    parser.add_argument(
        "--village-peak-threshold-frac",
        type=float,
        default=DEFAULT_VILLAGE_PEAK_THRESHOLD_FRAC,
    )
    parser.add_argument("--min-block-area", type=float, default=DEFAULT_MIN_BLOCK_AREA)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--no-points",
        action="store_true",
        help="Skip drawing world points in the overview render",
    )
    args = parser.parse_args(argv)

    manifest = run_macro(
        args.in_dir,
        args.out_dir,
        hierarchical=not args.legacy_seeding,
        n_cities=args.n_cities,
        merge_radius=args.merge_radius,
        peak_min_distance_units=args.peak_min_distance,
        peak_threshold_frac=args.peak_threshold_frac,
        village_merge_radius=args.village_merge_radius,
        village_peak_min_distance_units=args.village_peak_min_distance,
        village_peak_threshold_frac=args.village_peak_threshold_frac,
        cost_base=args.cost_base,
        w_slope=args.w_slope,
        w_river=args.w_river,
        w_density=args.w_density,
        cost_floor=args.cost_floor,
        beta_ratio=args.beta_ratio,
        simplify_tolerance=args.simplify_tolerance,
        min_block_area=args.min_block_area,
        seed=args.seed,
        draw_points=not args.no_points,
    )

    c = manifest["counts"]
    print("\n--- Macro stage 1 summary ---")
    print(f"  cities:       {c['n_cities']}")
    print(f"  towns:        {c['n_towns']}")
    print(f"  villages:     {c.get('n_villages', 0)}")
    print(f"  highways:     {c['n_highway_edges']}")
    print(f"  majors:       {c['n_major_edges']}")
    print(f"  locals:       {c.get('n_local_edges', 0)}")
    print(f"  macro-blocks: {c['n_macro_blocks']}")
    print(f"  runtime:      {manifest['runtime_seconds']}s")
    print(f"\nOutputs in: {args.out_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
