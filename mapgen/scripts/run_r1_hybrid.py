#!/usr/bin/env python
"""R1 stage 3 — full hybrid assembly: Chen/R2 in every macro-block.

Assembles the whole-island macro+micro hybrid from
``docs/large-scale-growth-research.md`` (staged build plan, step 3): the stage-1
macro arterial hierarchy (highways/majors + macro-blocks) with per-block
density-calibrated Chen/R2 *local* fabric run inside EVERY macro-block.

Connectivity (arterial<->local T-junctions) is a KNOWN, DELIBERATELY DEFERRED gap
(see the stage-2 seam findings): this script does NOT stitch junctions. The
deliverable is a full-island render at scale so the assembled character can be
judged before investing in seam stitching.

Calibration (LOCKED design decision; see the doc): a SINGLE GLOBAL district mass
``M = total_mass / total_target`` is applied as ``max_parcel_mass`` to every
block (dense blocks therefore split into more districts; district size stays
consistent island-wide), with a global absolute geometric floor
``min_parcel_area = island_area / (total_target * 4)`` so the mass gate, not
geometry, governs termination.

Outputs to mapgen/artifacts/r1/hybrid/ (or --out-dir):
  hybrid_overview.png   — full island: density backdrop + boundary + all Chen
                          district edges (faint) + Chen streets + macro
                          arterials (tier-colored, on top) + world points.
  hybrid_core{1..3}.png — zooms into the top-3 density peaks, same layers but
                          fabric/arterials thicker so local-meets-arterial reads.
  hybrid_manifest.json  — totals, calibration, per-block district counts, failure
                          count, runtime, invariant pass rate.

Run from mapgen/::
    uv run python scripts/run_r1_hybrid.py
    uv run python scripts/run_r1_hybrid.py --total-target 400
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
import shapely.geometry as sg  # noqa: E402

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "src") not in sys.path:
    sys.path.insert(0, str(_REPO / "src"))

from mapgen.r1_arm_a import IslandFields, build_density_field  # noqa: E402
from mapgen.r1_arm_b import find_density_peaks  # noqa: E402
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
    DEFAULT_RETRY_SEEDS,
    chen_in_block,
)
from mapgen.r1_zoom import load_points_with_labels, zoom_window  # noqa: E402

_DEFAULT_IN = _REPO / "artifacts/r1/inputs"
_DEFAULT_OUT = _REPO / "artifacts/r1/hybrid"

# Tier styling, matching run_r1_macro (highway red, major blue, local teal).
_TIER_STYLE: dict[int, dict[str, Any]] = {
    2: {"color": "#b30000", "label": "Highway (city–city)"},
    1: {"color": "#1f5fb0", "label": "Major (≥ town)"},
    0: {"color": "#138d75", "label": "Local (incl. village)"},
}


class BlockResult:
    """Per-block Chen outcome tagged with the macro-block id.

    A thin record (not a dataclass to keep it local): the districts and streets
    produced inside macro-block ``block_id``, the seed used, whether Chen
    succeeded (vs the single-district fallback), and the cheap invariant flags.
    """

    __slots__ = (
        "block_id",
        "districts",
        "streets",
        "failed",
        "seed_used",
        "district_count",
        "geometry_valid_pass",
        "paper_invariant_pass",
        "seconds",
    )

    def __init__(
        self,
        *,
        block_id: int,
        districts: list[sg.Polygon],
        streets: list[sg.LineString],
        failed: bool,
        seed_used: int | str,
        district_count: int,
        geometry_valid_pass: bool | None,
        paper_invariant_pass: bool | None,
        seconds: float,
    ) -> None:
        self.block_id = block_id
        self.districts = districts
        self.streets = streets
        self.failed = failed
        self.seed_used = seed_used
        self.district_count = district_count
        self.geometry_valid_pass = geometry_valid_pass
        self.paper_invariant_pass = paper_invariant_pass
        self.seconds = seconds


def run_all_blocks(
    macro_blocks: list[sg.Polygon],
    fields: IslandFields,
    *,
    max_parcel_mass: float,
    min_parcel_area: float,
    seeds: tuple[int, ...] = DEFAULT_RETRY_SEEDS,
) -> list[BlockResult]:
    """Run Chen/R2 inside every macro-block with the global calibration.

    Sequential (the user is OK with long runtimes). On total Chen failure for a
    block (every seed raised), fall back to leaving that block as a SINGLE
    district (the block polygon itself) and record it as failed, rather than
    aborting the whole run. Prints per-block progress.
    """
    results: list[BlockResult] = []
    n = len(macro_blocks)
    for i, block in enumerate(macro_blocks):
        t0 = time.perf_counter()
        res = chen_in_block(
            block,
            fields,
            max_parcel_mass=max_parcel_mass,
            min_parcel_area=min_parcel_area,
            seeds=seeds,
        )
        seconds = time.perf_counter() - t0
        if res.generated is None:
            # Fallback: the block itself is one district; no local streets.
            districts: list[sg.Polygon] = [block]
            streets: list[sg.LineString] = []
            failed = True
            district_count = 1
            geom_pass: bool | None = None
            inv_pass: bool | None = None
            seed_used: int | str = "all_failed"
            print(
                f"  block {i + 1}/{n}: FAILED Chen (fallback 1 district) "
                f"{seconds:.1f}s",
                flush=True,
            )
        else:
            districts = res.districts
            streets = res.streets
            failed = False
            district_count = res.info["district_count"]
            geom_pass = res.info.get("geometry_valid_pass")
            inv_pass = res.info.get("paper_invariant_pass")
            seed_used = res.info["seed_used"]
            print(
                f"  block {i + 1}/{n}: {district_count} districts "
                f"(seed {seed_used}) {seconds:.1f}s",
                flush=True,
            )
        results.append(
            BlockResult(
                block_id=i,
                districts=districts,
                streets=streets,
                failed=failed,
                seed_used=seed_used,
                district_count=district_count,
                geometry_valid_pass=geom_pass,
                paper_invariant_pass=inv_pass,
                seconds=round(seconds, 2),
            )
        )
    return results


def _color_lookup(l0_ids: np.ndarray) -> dict[int, tuple[float, float, float, float]]:
    unique = sorted({int(v) for v in l0_ids})
    cmap = plt.get_cmap("tab20")
    return {uid: cmap(i % 20) for i, uid in enumerate(unique)}


def _draw_layers(
    ax: Any,
    *,
    fields: IslandFields,
    boundary: sg.Polygon,
    results: list[BlockResult],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
    lp_xs: np.ndarray,
    lp_ys: np.ndarray,
    lp_l0: np.ndarray,
    colors: dict[int, tuple[float, float, float, float]],
    district_lw: float,
    district_alpha: float,
    street_lw: float,
    arterial_lw: dict[int, float],
    ring_lw: float,
    point_size: float,
    point_alpha: float,
) -> None:
    """Draw the shared hybrid layer stack on ``ax`` (backdrop -> arterials)."""
    density = fields.density
    nrows, ncols = density.shape
    x0, y0, cell = fields.x0, fields.y0, fields.cell

    _render_density_backdrop(
        ax, density, fields.height_carved, x0, y0, cell, nrows, ncols
    )
    _render_boundary(ax, boundary)

    # World points: tiny, low alpha, colored by l0 cluster.
    pt_colors = [colors[int(i)] for i in lp_l0]
    ax.scatter(
        lp_xs,
        lp_ys,
        s=point_size,
        c=pt_colors,
        alpha=point_alpha,
        linewidths=0.0,
        zorder=2,
    )

    # All Chen district edges: very thin, low alpha.
    for res in results:
        for poly in res.districts:
            if poly.is_empty:
                continue
            for ring in [poly.exterior, *list(poly.interiors)]:
                rx, ry = ring.xy
                ax.plot(
                    rx,
                    ry,
                    color="#555555",
                    lw=district_lw,
                    alpha=district_alpha,
                    zorder=3,
                )

    # Chen streets: thin black.
    for res in results:
        for line in res.streets:
            if line.geom_type != "LineString":
                continue
            lx, ly = line.xy
            ax.plot(lx, ly, color="#1a1a1a", lw=street_lw, alpha=0.85, zorder=4)

    # Macro arterials on top, tier-colored (locals, then majors, then highways).
    for tier in (0, 1, 2):
        if tier not in _TIER_STYLE or tier not in arterial_lw:
            continue
        style = _TIER_STYLE[tier]
        for line, rec in zip(arterial_lines, edges, strict=True):
            if rec.tier != tier or line.geom_type != "LineString":
                continue
            lx, ly = line.xy
            ax.plot(
                lx,
                ly,
                color=style["color"],
                lw=arterial_lw[tier],
                alpha=0.95,
                zorder=5 + tier,
                solid_capstyle="round",
            )

    # Core ring roads on top: dashed dark-orange downtown rings.
    for poly in core_polys:
        if poly.is_empty:
            continue
        rx, ry = poly.exterior.xy
        ax.plot(
            rx,
            ry,
            color="#d35400",
            lw=ring_lw,
            ls=(0, (5, 2)),
            alpha=0.95,
            zorder=9,
            solid_capstyle="round",
        )


def _legend_handles() -> list[Any]:
    return [
        mpatches.Patch(color=_TIER_STYLE[2]["color"], label=_TIER_STYLE[2]["label"]),
        mpatches.Patch(color=_TIER_STYLE[1]["color"], label=_TIER_STYLE[1]["label"]),
        mpatches.Patch(color=_TIER_STYLE[0]["color"], label=_TIER_STYLE[0]["label"]),
        plt.Line2D(
            [0], [0], color="#d35400", lw=2.0, ls=(0, (5, 2)), label="Core ring road"
        ),
        plt.Line2D([0], [0], color="#1a1a1a", lw=1.2, label="Chen street (local)"),
        plt.Line2D([0], [0], color="#555555", lw=0.8, label="Chen district edge"),
    ]


def render_overview(
    *,
    fields: IslandFields,
    boundary: sg.Polygon,
    results: list[BlockResult],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
    lp_xs: np.ndarray,
    lp_ys: np.ndarray,
    lp_l0: np.ndarray,
    colors: dict[int, tuple[float, float, float, float]],
    total_districts: int,
    total_streets: int,
    out_path: Path,
    dpi: int,
) -> None:
    """Full-island hybrid overview render."""
    fig, ax = plt.subplots(figsize=(18, 13))
    _draw_layers(
        ax,
        fields=fields,
        boundary=boundary,
        results=results,
        arterial_lines=arterial_lines,
        edges=edges,
        core_polys=core_polys,
        lp_xs=lp_xs,
        lp_ys=lp_ys,
        lp_l0=lp_l0,
        colors=colors,
        district_lw=0.25,
        district_alpha=0.35,
        street_lw=0.4,
        arterial_lw={2: 2.6, 1: 1.2, 0: 0.6},
        ring_lw=1.6,
        point_size=1.2,
        point_alpha=0.18,
    )
    _set_extent(ax, boundary, pad=2.0)
    n_hwy = sum(1 for e in edges if e.tier == 2)
    n_major = sum(1 for e in edges if e.tier == 1)
    n_local = sum(1 for e in edges if e.tier == 0)
    ax.set_title(
        "R1 hybrid — macro arterials + core ring-roads + per-block Chen fabric\n"
        f"{len(results)} macro-blocks, {len(core_polys)} core rings, "
        f"{total_districts} districts, {total_streets} Chen streets, "
        f"{n_hwy} highways, {n_major} majors, {n_local} locals",
        fontsize=13,
    )
    ax.legend(handles=_legend_handles(), loc="upper left", fontsize=9, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def render_core(
    *,
    fields: IslandFields,
    boundary: sg.Polygon,
    results: list[BlockResult],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
    lp_xs: np.ndarray,
    lp_ys: np.ndarray,
    lp_l0: np.ndarray,
    colors: dict[int, tuple[float, float, float, float]],
    window: tuple[float, float, float, float],
    rank: int,
    cx: float,
    cy: float,
    out_path: Path,
    dpi: int,
) -> None:
    """Dense-core zoom render (thicker fabric/arterials than the overview)."""
    fig, ax = plt.subplots(figsize=(11, 11))
    _draw_layers(
        ax,
        fields=fields,
        boundary=boundary,
        results=results,
        arterial_lines=arterial_lines,
        edges=edges,
        core_polys=core_polys,
        lp_xs=lp_xs,
        lp_ys=lp_ys,
        lp_l0=lp_l0,
        colors=colors,
        district_lw=0.6,
        district_alpha=0.7,
        street_lw=1.1,
        arterial_lw={2: 3.6, 1: 2.2, 0: 1.3},
        ring_lw=2.8,
        point_size=14.0,
        point_alpha=0.85,
    )
    ax.set_xlim(window[0], window[2])
    ax.set_ylim(window[1], window[3])
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(
        f"hybrid dense core #{rank} @ ({cx:.0f}, {cy:.0f}) — "
        "local Chen fabric meeting macro arterials",
        fontsize=12,
    )
    ax.legend(handles=_legend_handles(), loc="upper left", fontsize=8, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def run_hybrid(
    in_dir: Path,
    out_dir: Path,
    *,
    total_target: int,
    n_cores: int = 3,
    dpi: int = 240,
) -> dict[str, Any]:
    """Full hybrid assembly: macro layer -> per-block Chen -> assemble -> render."""
    t_start = time.perf_counter()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Building stage-1 macro layer (shared build_macro_layer)…", flush=True)
    fields = IslandFields.from_npz(str(in_dir / "fields.npz"))
    boundary = load_boundary(in_dir)
    points = pl.read_parquet(in_dir / "island_points.parquet")
    params = MacroParams()
    # Stage 3.5b: the block set is polygonized over CLIPPED arterials + core
    # rings + boundary, and the drawn arterials are the clipped ones (no
    # convergence on a core summit).
    layer = build_macro_layer(fields, boundary, points, params)
    arterial_lines: list[sg.LineString] = layer.arterial_lines
    edges: list[MacroEdge] = layer.edges
    core_polys: list[sg.Polygon] = layer.core_polys
    macro_blocks: list[sg.Polygon] = layer.blocks
    n_blocks = len(macro_blocks)
    print(
        f"  {n_blocks} macro-blocks, {len(core_polys)} core rings, "
        f"{len(edges)} arterial segments",
        flush=True,
    )

    # Global calibration (LOCKED): single district mass M over every block.
    density_field = build_density_field(fields)
    total_mass = float(density_field.mass(boundary))
    island_area = float(boundary.area)
    global_m = total_mass / float(total_target)
    min_parcel_area = island_area / (float(total_target) * 4.0)
    print(
        f"  calibration: total_mass={total_mass:.2f}, island_area={island_area:.2f}, "
        f"M={global_m:.4f}, min_parcel_area={min_parcel_area:.4f}",
        flush=True,
    )

    print(f"Running Chen/R2 in {n_blocks} macro-blocks (global M)…", flush=True)
    results = run_all_blocks(
        macro_blocks,
        fields,
        max_parcel_mass=global_m,
        min_parcel_area=min_parcel_area,
    )

    total_districts = sum(r.district_count for r in results)
    total_streets = sum(len(r.streets) for r in results)
    n_failed = sum(1 for r in results if r.failed)
    print(
        f"  assembled: {total_districts} districts, {total_streets} Chen streets, "
        f"{n_failed} blocks failed",
        flush=True,
    )

    # Points (loaded once, reused across renders).
    lp = load_points_with_labels(str(in_dir / "island_points.parquet"))
    colors = _color_lookup(lp.l0_ids)

    print("Rendering hybrid_overview.png…", flush=True)
    render_overview(
        fields=fields,
        boundary=boundary,
        results=results,
        arterial_lines=arterial_lines,
        edges=edges,
        core_polys=core_polys,
        lp_xs=lp.xs,
        lp_ys=lp.ys,
        lp_l0=lp.l0_ids,
        colors=colors,
        total_districts=total_districts,
        total_streets=total_streets,
        out_path=out_dir / "hybrid_overview.png",
        dpi=dpi,
    )

    # Dense-core zooms into the top density peaks.
    density = fields.density
    x0, y0, cell = fields.x0, fields.y0, fields.cell
    peak_xs, peak_ys, peak_ds = find_density_peaks(density, x0, y0, cell)
    order = np.argsort(peak_ds)[::-1]
    span = max(
        boundary.bounds[2] - boundary.bounds[0],
        boundary.bounds[3] - boundary.bounds[1],
    )
    half_span = span * 0.12
    core_paths: list[str] = []
    for rank, pidx in enumerate(order[:n_cores], start=1):
        cx, cy = float(peak_xs[pidx]), float(peak_ys[pidx])
        window = zoom_window(cx, cy, half_span)
        out_path = out_dir / f"hybrid_core{rank}.png"
        print(f"Rendering {out_path.name}…", flush=True)
        render_core(
            fields=fields,
            boundary=boundary,
            results=results,
            arterial_lines=arterial_lines,
            edges=edges,
            core_polys=core_polys,
            lp_xs=lp.xs,
            lp_ys=lp.ys,
            lp_l0=lp.l0_ids,
            colors=colors,
            window=window,
            rank=rank,
            cx=cx,
            cy=cy,
            out_path=out_path,
            dpi=dpi,
        )
        core_paths.append(str(out_path))

    runtime_s = round(time.perf_counter() - t_start, 2)

    n_hwy = sum(1 for e in edges if e.tier == 2)
    n_major = sum(1 for e in edges if e.tier == 1)
    n_local = sum(1 for e in edges if e.tier == 0)

    # Invariant pass rate over the blocks where Chen actually ran (non-fallback).
    ran = [r for r in results if not r.failed]
    n_geom_pass = sum(1 for r in ran if r.geometry_valid_pass)
    n_inv_pass = sum(1 for r in ran if r.paper_invariant_pass)
    pass_rate = {
        "n_blocks_chen_ran": len(ran),
        "geometry_valid_pass": n_geom_pass,
        "paper_invariant_pass": n_inv_pass,
        "geometry_valid_pass_frac": round(n_geom_pass / len(ran), 4) if ran else None,
        "paper_invariant_pass_frac": round(n_inv_pass / len(ran), 4) if ran else None,
    }

    manifest: dict[str, Any] = {
        "stage": "3 (full hybrid assembly, stage-3.5b core ring-roads)",
        "description": (
            "Chen/R2 in every macro-block (global district mass) + macro "
            "arterials clipped to dense-core ring-roads (downtown blocks); "
            "arterial<->local junctions deliberately deferred"
        ),
        "macro_params": params.to_dict(),
        "total_target": total_target,
        "M": round(global_m, 6),
        "min_parcel_area": round(min_parcel_area, 6),
        "total_mass": round(total_mass, 4),
        "island_area": round(island_area, 4),
        "n_core_rings": len(core_polys),
        "n_macro_blocks": n_blocks,
        "n_blocks_failed": n_failed,
        "failed_block_ids": [r.block_id for r in results if r.failed],
        "total_districts": total_districts,
        "total_chen_streets": total_streets,
        "n_highway_arterials": n_hwy,
        "n_major_arterials": n_major,
        "n_local_arterials": n_local,
        "per_block_district_counts": [r.district_count for r in results],
        "per_block_seed_used": [r.seed_used for r in results],
        "invariant_pass_rate": pass_rate,
        "retry_seeds": list(DEFAULT_RETRY_SEEDS),
        "runtime_seconds": runtime_s,
        "outputs": [
            "hybrid_overview.png",
            *[Path(p).name for p in core_paths],
            "hybrid_manifest.json",
        ],
    }
    with (out_dir / "hybrid_manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote hybrid_manifest.json (runtime {runtime_s}s)", flush=True)
    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R1 stage 3: full hybrid assembly (Chen/R2 in every block)"
    )
    parser.add_argument("--in-dir", type=Path, default=_DEFAULT_IN)
    parser.add_argument("--out-dir", type=Path, default=_DEFAULT_OUT)
    parser.add_argument(
        "--total-target",
        type=int,
        default=1200,
        help="Island-wide district target; sets global M = total_mass / target",
    )
    parser.add_argument("--n-cores", type=int, default=3)
    parser.add_argument("--dpi", type=int, default=240)
    args = parser.parse_args(argv)

    manifest = run_hybrid(
        args.in_dir,
        args.out_dir,
        total_target=args.total_target,
        n_cores=args.n_cores,
        dpi=args.dpi,
    )

    print("\n--- Hybrid stage 3 summary ---")
    print(f"  total_target:    {manifest['total_target']}")
    print(f"  M (district):    {manifest['M']}")
    print(f"  macro-blocks:    {manifest['n_macro_blocks']}")
    print(f"  blocks failed:   {manifest['n_blocks_failed']}")
    print(f"  total districts: {manifest['total_districts']}")
    print(f"  Chen streets:    {manifest['total_chen_streets']}")
    print(f"  runtime:         {manifest['runtime_seconds']}s")
    print(f"\nOutputs in: {args.out_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
