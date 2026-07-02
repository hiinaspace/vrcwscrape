#!/usr/bin/env python
"""R1 stage 2 — seam spike: Chen/R2 inside ONE macro-block.

A viability probe (not a finished pipeline) for the macro+micro hybrid in
``docs/large-scale-growth-research.md`` (staged build plan, step 2). It rebuilds
the stage-1 macro layer via the shared ``mapgen.r1_macro.build_macro_layer``
with default ``MacroParams`` (hierarchical 3-tier seeding + dense-core
ring-roads — the SAME layer as ``run_r1_macro`` / ``run_r1_hybrid``), selects
one mid-complexity macro-block, runs Chen/R2 *inside* that block, and measures /
renders the **seam**: how cleanly the local street fabric meets the bounding
arterials.

Outputs to mapgen/artifacts/r1/seam/ (or --out-dir):
  seam_overview.png  — full island, density + all arterials (faint, tiered),
                       with the chosen block highlighted. Orientation shot.
  seam_block.png     — zoom on the chosen block: bounding arterials (thick,
                       tiered), Chen districts + streets inside, world points by
                       l0 cluster. THIS shows the seam.
  seam_manifest.json — block id/area, world count, target, seed used,
                       district/street counts, invariant passes, seam metrics,
                       runtime.

Run from mapgen/::
    uv run python scripts/run_r1_seam_spike.py
    uv run python scripts/run_r1_seam_spike.py --block-frac 0.5 --target 40
    uv run python scripts/run_r1_seam_spike.py --block-id 7
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

from mapgen.chen_artifacts import _street_lines  # noqa: E402
from mapgen.chen_generate import GeneratedChenLayout  # noqa: E402
from mapgen.r1_arm_a import (  # noqa: E402
    IslandFields,
    build_density_field,
    default_max_parcel_mass,
)
from mapgen.r1_compare import (  # noqa: E402
    _render_boundary,
    _render_density_backdrop,
    _set_extent,
)
from mapgen.r1_macro import (  # noqa: E402
    MacroEdge,
    MacroParams,
    build_macro_layer,
    load_boundary,
)
from mapgen.r1_seam import (  # noqa: E402
    SeamGap,
    chen_in_block,
    seam_gap,
    select_block_by_rank,
)
from mapgen.r1_zoom import load_points_with_labels  # noqa: E402

_DEFAULT_IN = _REPO / "artifacts/r1/inputs"
_DEFAULT_OUT = _REPO / "artifacts/r1/seam"

# Seeds tried in order; a weird concave block breaking Chen on seed 7 but working
# on a later seed IS a finding, recorded in the manifest.
_RETRY_SEEDS: tuple[int, ...] = (7, 1, 2, 13)

# Tier styling, matching run_r1_macro (highway = thick warm, major = cool).
_TIER_STYLE: dict[int, dict[str, Any]] = {
    2: {"color": "#b30000", "lw": 1.2, "label": "Highway (city–city)"},
    1: {"color": "#1f5fb0", "lw": 0.7, "label": "Major (city–town / town–town)"},
    0: {"color": "#138d75", "lw": 0.5, "label": "Local (incl. village)"},
}


def run_chen_in_block(
    block: sg.Polygon,
    fields: IslandFields,
    *,
    target: int,
    seeds: tuple[int, ...] = _RETRY_SEEDS,
) -> tuple[GeneratedChenLayout | None, dict[str, Any]]:
    """Run Chen/R2 (density-mass mode) inside ``block``, retrying across seeds.

    Mirrors the R2 call in ``run_r1_zoom_review.py`` but bounded to one block.
    Returns ``(generated_or_None, info)`` where ``info`` records the seed that
    worked (or ``"all_failed"``) and per-seed errors.
    """
    density_field = build_density_field(fields)
    parcel_count = max(target * 2, target + 1)

    # SEAM FINDING (calibration seam): the island-wide
    # default_max_parcel_mass(density_field, target) = total_island_mass / target
    # sizes a parcel against the WHOLE island. One macro-block holds only a small
    # fraction of that mass (~2% here), so its entire mass falls below a single
    # parcel's island-wide allowance and the density-mass gate never splits ->
    # 1 district. To make ``target`` mean "districts INSIDE this block", we
    # calibrate the mass cap to the block's own integrated mass. Both values are
    # recorded so the discrepancy is visible.
    island_max_parcel_mass = default_max_parcel_mass(density_field, target)
    block_mass = float(density_field.mass(block))
    if block_mass > 0:
        max_parcel_mass = block_mass / float(target)
    else:
        # Degenerate (no density in block): fall back to the island calibration.
        max_parcel_mass = island_max_parcel_mass

    # Preserve the original ``parcel_count`` -> ``min_parcel_area`` mapping that
    # ``generate_layout_for_boundary`` applies (area / (1.5 * parcel_count)), so
    # delegating to the shared ``chen_in_block`` helper is behavior-preserving.
    min_parcel_area = float(block.area) / (1.5 * float(parcel_count))

    result = chen_in_block(
        block,
        fields,
        max_parcel_mass=max_parcel_mass,
        min_parcel_area=min_parcel_area,
        seeds=seeds,
    )

    info: dict[str, Any] = {
        "target": target,
        "parcel_count_floor": parcel_count,
        "block_mass": round(block_mass, 4),
        "max_parcel_mass": round(float(max_parcel_mass), 4),
        "island_default_max_parcel_mass": round(float(island_max_parcel_mass), 4),
        "max_parcel_mass_calibration": "block" if block_mass > 0 else "island_fallback",
        "seeds_tried": result.info["seeds_tried"],
        "seed_used": result.info["seed_used"],
    }
    if result.generated is not None:
        info["district_count"] = result.info["district_count"]
        info["geometry_valid_pass"] = result.info["geometry_valid_pass"]
        info["paper_invariant_pass"] = result.info["paper_invariant_pass"]
    return result.generated, info


def _district_polys(generated: GeneratedChenLayout) -> list[sg.Polygon]:
    return [parcel.geom for parcel in generated.layout.mesh.parcels.values()]


def _chen_street_lines(generated: GeneratedChenLayout) -> list[sg.LineString]:
    return [line for line, _props in _street_lines(generated.layout)]


def _bounding_arterials(
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    block: sg.Polygon,
) -> list[tuple[sg.LineString, MacroEdge]]:
    """Arterials whose geometry touches the block boundary (the seam edges)."""
    ring = block.exterior
    out: list[tuple[sg.LineString, MacroEdge]] = []
    for line, rec in zip(arterial_lines, edges, strict=True):
        if line.intersects(ring) or line.intersects(block):
            out.append((line, rec))
    return out


def _color_lookup(l0_ids: np.ndarray) -> dict[int, tuple[float, float, float, float]]:
    unique = sorted({int(v) for v in l0_ids})
    cmap = plt.get_cmap("tab20")
    return {uid: cmap(i % 20) for i, uid in enumerate(unique)}


def render_overview(
    *,
    fields: IslandFields,
    boundary: sg.Polygon,
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    block: sg.Polygon,
    block_id: int,
    out_path: Path,
    dpi: int,
) -> None:
    """Full-island orientation shot with the chosen block highlighted."""
    density = fields.density
    nrows, ncols = density.shape
    x0, y0, cell = fields.x0, fields.y0, fields.cell

    fig, ax = plt.subplots(figsize=(13, 9))
    _render_density_backdrop(
        ax, density, fields.height_carved, x0, y0, cell, nrows, ncols
    )
    _render_boundary(ax, boundary)

    # All macro arterials, faint, tier-colored (locals first, highways on top).
    for tier in (0, 1, 2):
        style = _TIER_STYLE[tier]
        for line, rec in zip(arterial_lines, edges, strict=True):
            if rec.tier != tier or line.geom_type != "LineString":
                continue
            lx, ly = line.xy
            ax.plot(
                lx,
                ly,
                color=style["color"],
                lw=style["lw"],
                alpha=0.45,
                zorder=4 + tier,
                solid_capstyle="round",
            )

    # Chosen block: highlight fill + outline.
    bx, by = block.exterior.xy
    ax.fill(bx, by, color="#00c2a8", alpha=0.30, zorder=6)
    ax.plot(bx, by, color="#007f6e", lw=2.2, zorder=7)

    _set_extent(ax, boundary, pad=2.0)
    ax.set_title(
        f"R1 seam spike — chosen macro-block #{block_id} "
        f"(area {block.area:.1f}) on the stage-1 macro layer",
        fontsize=11,
    )
    legend_handles = [
        mpatches.Patch(color=_TIER_STYLE[2]["color"], label=_TIER_STYLE[2]["label"]),
        mpatches.Patch(color=_TIER_STYLE[1]["color"], label=_TIER_STYLE[1]["label"]),
        mpatches.Patch(color=_TIER_STYLE[0]["color"], label=_TIER_STYLE[0]["label"]),
        mpatches.Patch(color="#00c2a8", alpha=0.5, label=f"Chosen block #{block_id}"),
    ]
    ax.legend(handles=legend_handles, loc="upper left", fontsize=8, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def render_block(
    *,
    fields: IslandFields,
    block: sg.Polygon,
    block_id: int,
    bounding_arterials: list[tuple[sg.LineString, MacroEdge]],
    districts: list[sg.Polygon],
    chen_streets: list[sg.LineString],
    points_path: Path,
    gap: SeamGap,
    out_path: Path,
    dpi: int,
    pad: float = 3.0,
) -> None:
    """Zoomed seam view: bounding arterials vs Chen fabric at the block edge."""
    density = fields.density
    nrows, ncols = density.shape
    x0, y0, cell = fields.x0, fields.y0, fields.cell
    minx, miny, maxx, maxy = block.bounds

    fig, ax = plt.subplots(figsize=(11, 10))
    _render_density_backdrop(
        ax, density, fields.height_carved, x0, y0, cell, nrows, ncols
    )

    # Chen districts inside (thin gray edges, very light fill).
    for poly in districts:
        if poly.is_empty:
            continue
        rings = [poly.exterior, *list(poly.interiors)]
        for ring in rings:
            rx, ry = ring.xy
            ax.fill(rx, ry, color="#bbbbbb", alpha=0.10, zorder=2)
            ax.plot(rx, ry, color="#555555", lw=0.6, alpha=0.9, zorder=3)

    # Chen streets inside (medium).
    for line in chen_streets:
        if line.geom_type != "LineString":
            continue
        lx, ly = line.xy
        ax.plot(lx, ly, color="#222222", lw=1.0, alpha=0.9, zorder=4)

    # World points within the block, colored by l0 cluster.
    n_worlds = 0
    if points_path.exists():
        lp = load_points_with_labels(str(points_path))
        colors = _color_lookup(lp.l0_ids)
        inside = shapely_contains_block(block, lp.xs, lp.ys)
        n_worlds = int(inside.sum())
        if n_worlds > 0:
            pt_colors = [colors[int(i)] for i in lp.l0_ids[inside]]
            ax.scatter(
                lp.xs[inside],
                lp.ys[inside],
                s=16,
                c=pt_colors,
                alpha=0.9,
                linewidths=0.2,
                edgecolors="white",
                zorder=6,
            )

    # Bounding arterials, thick + tier-colored, on top.
    for line, rec in bounding_arterials:
        if line.geom_type != "LineString":
            continue
        style = _TIER_STYLE.get(rec.tier, _TIER_STYLE[1])
        lx, ly = line.xy
        ax.plot(
            lx,
            ly,
            color=style["color"],
            lw=3.2 if rec.tier == 2 else 2.2,
            alpha=0.95,
            zorder=8,
            solid_capstyle="round",
        )

    # Block boundary, bold, on very top.
    bx, by = block.exterior.xy
    ax.plot(bx, by, color="#007f6e", lw=2.6, zorder=9)

    ax.set_xlim(minx - pad, maxx + pad)
    ax.set_ylim(miny - pad, maxy + pad)
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(
        f"seam @ macro-block #{block_id} — area {block.area:.1f}, "
        f"{len(districts)} Chen districts, {n_worlds} worlds\n"
        f"seam gap: max {gap.max_boundary_gap:.2f}, mean "
        f"{gap.mean_boundary_gap:.2f}, uncovered "
        f"{gap.uncovered_frac * 100:.1f}%",
        fontsize=11,
    )
    legend_handles = [
        mpatches.Patch(color=_TIER_STYLE[2]["color"], label="Highway (bounding)"),
        mpatches.Patch(color=_TIER_STYLE[1]["color"], label="Major (bounding)"),
        plt.Line2D([0], [0], color="#222222", lw=1.2, label="Chen street"),
        plt.Line2D([0], [0], color="#555555", lw=0.8, label="Chen district edge"),
        plt.Line2D([0], [0], color="#007f6e", lw=2.0, label="Block boundary"),
    ]
    ax.legend(handles=legend_handles, loc="upper left", fontsize=8, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def shapely_contains_block(
    block: sg.Polygon, xs: np.ndarray, ys: np.ndarray
) -> np.ndarray:
    """Boolean mask of points (xs, ys) inside ``block`` (vectorized)."""
    return np.asarray(shapely.contains_xy(block, xs, ys), dtype=bool)


def run_seam(
    in_dir: Path,
    out_dir: Path,
    *,
    block_frac: float,
    block_id: int | None,
    target: int,
    dpi: int = 220,
) -> dict[str, Any]:
    """Full seam spike: macro layer -> block select -> Chen -> metric -> render."""
    t_start = time.perf_counter()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Building stage-1 macro layer (shared build_macro_layer)…", flush=True)
    fields = IslandFields.from_npz(str(in_dir / "fields.npz"))
    boundary = load_boundary(in_dir)
    points = pl.read_parquet(in_dir / "island_points.parquet")
    params = MacroParams()
    layer = build_macro_layer(fields, boundary, points, params)
    arterial_lines: list[sg.LineString] = layer.arterial_lines
    edges: list[MacroEdge] = layer.edges
    macro_blocks: list[sg.Polygon] = layer.blocks
    print(f"  {len(macro_blocks)} macro-blocks, {len(edges)} arterials", flush=True)

    if block_id is not None:
        if not 0 <= block_id < len(macro_blocks):
            raise ValueError(
                f"--block-id {block_id} out of range [0, {len(macro_blocks)})"
            )
        chosen_id = block_id
        block = macro_blocks[block_id]
        print(f"  using explicit --block-id {chosen_id}", flush=True)
    else:
        chosen_id, block = select_block_by_rank(
            macro_blocks, block_frac, min_block_area=params.min_block_area
        )
        print(
            f"  selected block #{chosen_id} at frac {block_frac} "
            f"(area {block.area:.1f})",
            flush=True,
        )

    print(f"Running Chen/R2 inside block #{chosen_id} (target {target})…", flush=True)
    generated, chen_info = run_chen_in_block(block, fields, target=target)
    if generated is None:
        print(f"  Chen FAILED on all seeds {_RETRY_SEEDS}", flush=True)
        districts: list[sg.Polygon] = []
        chen_streets: list[sg.LineString] = []
    else:
        print(
            f"  Chen ok on seed {chen_info['seed_used']}: "
            f"{chen_info['district_count']} districts",
            flush=True,
        )
        districts = _district_polys(generated)
        chen_streets = _chen_street_lines(generated)

    gap = seam_gap(districts, block)
    print(
        f"  seam gap: max {gap.max_boundary_gap:.3f}, "
        f"uncovered {gap.uncovered_frac * 100:.1f}%",
        flush=True,
    )

    bounding = _bounding_arterials(arterial_lines, edges, block)

    # World count inside block (independent of render path).
    lp = load_points_with_labels(str(in_dir / "island_points.parquet"))
    n_worlds = int(shapely_contains_block(block, lp.xs, lp.ys).sum())

    print("Rendering seam_overview.png…", flush=True)
    render_overview(
        fields=fields,
        boundary=boundary,
        arterial_lines=arterial_lines,
        edges=edges,
        block=block,
        block_id=chosen_id,
        out_path=out_dir / "seam_overview.png",
        dpi=dpi,
    )
    print("Rendering seam_block.png…", flush=True)
    render_block(
        fields=fields,
        block=block,
        block_id=chosen_id,
        bounding_arterials=bounding,
        districts=districts,
        chen_streets=chen_streets,
        points_path=in_dir / "island_points.parquet",
        gap=gap,
        out_path=out_dir / "seam_block.png",
        dpi=dpi,
    )

    runtime_s = round(time.perf_counter() - t_start, 3)
    manifest: dict[str, Any] = {
        "stage": "2 (seam spike)",
        "description": "Chen/R2 inside one macro-block; measure local-vs-arterial seam",
        "macro_params": params.to_dict(),
        "block_id": chosen_id,
        "block_area": round(float(block.area), 3),
        "block_frac": None if block_id is not None else block_frac,
        "n_worlds_in_block": n_worlds,
        "target": target,
        "seed_used": chen_info["seed_used"],
        "n_chen_districts": len(districts),
        "n_chen_streets": len(chen_streets),
        "n_bounding_arterials": len(bounding),
        "geometry_valid_pass": chen_info.get("geometry_valid_pass"),
        "paper_invariant_pass": chen_info.get("paper_invariant_pass"),
        "seam_gap": gap.to_dict(),
        "chen_run": chen_info,
        "runtime_seconds": runtime_s,
        "outputs": ["seam_overview.png", "seam_block.png", "seam_manifest.json"],
    }
    with (out_dir / "seam_manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote seam_manifest.json (runtime {runtime_s}s)", flush=True)
    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R1 stage 2: seam spike (Chen/R2 inside one macro-block)"
    )
    parser.add_argument("--in-dir", type=Path, default=_DEFAULT_IN)
    parser.add_argument("--out-dir", type=Path, default=_DEFAULT_OUT)
    parser.add_argument(
        "--block-frac",
        type=float,
        default=0.5,
        help="Fractional area rank of the chosen block among non-slivers (0.5=median)",
    )
    parser.add_argument(
        "--block-id",
        type=int,
        default=None,
        help="Explicit macro-block index override (skips fractional selection)",
    )
    parser.add_argument("--target", type=int, default=40)
    parser.add_argument("--dpi", type=int, default=220)
    args = parser.parse_args(argv)

    manifest = run_seam(
        args.in_dir,
        args.out_dir,
        block_frac=args.block_frac,
        block_id=args.block_id,
        target=args.target,
        dpi=args.dpi,
    )

    print("\n--- Seam spike summary ---")
    print(f"  block:        #{manifest['block_id']} (area {manifest['block_area']})")
    print(f"  worlds:       {manifest['n_worlds_in_block']}")
    print(f"  seed used:    {manifest['seed_used']}")
    print(f"  districts:    {manifest['n_chen_districts']}")
    print(f"  streets:      {manifest['n_chen_streets']}")
    print(f"  seam gap:     {manifest['seam_gap']}")
    print(f"  runtime:      {manifest['runtime_seconds']}s")
    print(f"\nOutputs in: {args.out_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
