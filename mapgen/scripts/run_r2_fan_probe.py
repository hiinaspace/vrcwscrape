#!/usr/bin/env python
"""R2 fan probe — one-knob ablation of the summit sliver-fan defect.

Diagnosis probe for the "parallel-sliver district fans over density summits"
defect flagged by the 2026-07-01 PR #1 re-review (see
``docs/large-scale-growth-research.md``): the hybrid's per-block Chen/R2 run
(``r1_seam.chen_in_block``) produces 10+ long thin near-parallel districts
draped over density peaks, including inside core rings. Working hypothesis: the
R2 density-mass gate forces many splits exactly where density is high, while
the ridge-aligned guidance field makes candidate cuts run along the ridge;
possibly compounded by the regional split weights down-weighting regularity.

This script varies ONE knob at a time from the hybrid's exact per-block config
on 3 test blocks (2 core-ring downtowns + 1 combed non-core band):

  A. control — exact hybrid config (must reproduce the fan)
  B. paper split weights (0.30/0.50/0.20) instead of REGIONAL_SPLIT_WEIGHTS
  C. guidance=None
  D. no density-mass mode; ``parcel_count`` set to A's district count
  E. guidance at half strength (strength 6->3, density_ridge_boost 2->1;
     the guidance weight raster is linear in both, so this exactly halves it)
  F. B + C combined (paper weights AND no guidance)

Per (block, config): district count, minimum-rotated-rectangle elongation
distribution (share > 4, share > 6, max, median), mean Chen irregularity.
Outputs to mapgen/artifacts/r1/fan_probe/:
  fan_probe_metrics.json     — calibration, chosen blocks, full metrics table.
  block<id>_matrix.png       — per block: 6-config contact sheet (district
                               edges + streets over the density backdrop).

Run from mapgen/::
    uv run python scripts/run_r2_fan_probe.py
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import polars as pl  # noqa: E402
import shapely.geometry as sg  # noqa: E402

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "src") not in sys.path:
    sys.path.insert(0, str(_REPO / "src"))

from mapgen.chen_artifacts import _street_lines  # noqa: E402
from mapgen.chen_generate import (  # noqa: E402
    BoundarySpec,
    generate_layout_for_boundary,
)
from mapgen.chen_streets import StreetConfig  # noqa: E402
from mapgen.r1_arm_a import (  # noqa: E402
    REGIONAL_SPLIT_WEIGHTS,
    IslandFields,
    build_density_field,
    build_terrain_guidance,
)
from mapgen.r1_compare import _render_density_backdrop  # noqa: E402
from mapgen.r1_macro import MacroParams, build_macro_layer, load_boundary  # noqa: E402
from mapgen.r1_seam import DEFAULT_RETRY_SEEDS  # noqa: E402

_DEFAULT_IN = _REPO / "artifacts/r1/inputs"
_DEFAULT_OUT = _REPO / "artifacts/r1/fan_probe"

# Hybrid's chen_in_block guidance defaults (mirrored; keep in sync manually —
# this probe deliberately does not modify r1_seam).
_HYBRID_GUIDANCE_STRENGTH = 6.0
_HYBRID_DENSITY_RIDGE_BOOST = 2.0

# (key, short label). Order fixed: A must run first (D calibrates against it).
_CONFIGS: tuple[tuple[str, str], ...] = (
    ("A", "control (hybrid exact)"),
    ("B", "paper split weights"),
    ("C", "no guidance"),
    ("D", "no mass mode (count≈A)"),
    ("E", "guidance half strength"),
    ("F", "paper weights + no guidance"),
)

# The three probe targets: island-frame points + role labels (see module doc).
_CORE_POINTS: tuple[tuple[str, float, float], ...] = (
    ("core1_downtown", 155.0, 48.0),
    ("core2_downtown", 138.0, 40.0),
)
_NONCORE_POINT: tuple[float, float] = (95.0, 63.0)
_NONCORE_SCAN_XS: tuple[float, ...] = tuple(float(x) for x in range(85, 106))
_NONCORE_SCAN_Y: float = 63.0


class _CellTimeout(Exception):
    """Raised by SIGALRM when a (block, config) cell exceeds the budget."""


def _alarm(_signum: int, _frame: Any) -> None:
    raise _CellTimeout()


# ---------------------------------------------------------------------------
# Block selection
# ---------------------------------------------------------------------------


def _block_containing(blocks: list[sg.Polygon], x: float, y: float) -> int | None:
    """Index of the first block containing island-frame point (x, y), else None."""
    pt = sg.Point(x, y)
    for i in range(len(blocks)):  # index order: deterministic
        if blocks[i].contains(pt):
            return i
    return None


def _is_core_block(block: sg.Polygon, core_polys: list[sg.Polygon]) -> bool:
    """A block is a core (downtown) block if a core ring covers most of it."""
    area = float(block.area)
    if area <= 0.0:
        return False
    return any(float(block.intersection(core).area) > 0.5 * area for core in core_polys)


def select_probe_blocks(
    blocks: list[sg.Polygon],
    core_polys: list[sg.Polygon],
    density_field: Any,
) -> list[dict[str, Any]]:
    """Pick the 3 test blocks (2 downtown cores + 1 non-core combed band).

    Returns [{role, block_id, point, area, mass, is_core}, ...] in probe order.
    Raises RuntimeError if a target point misses (with what WAS found, so the
    probe fails loudly rather than silently probing the wrong fabric).
    """
    chosen: list[dict[str, Any]] = []

    for role, x, y in _CORE_POINTS:
        idx = _block_containing(blocks, x, y)
        if idx is None:
            raise RuntimeError(f"{role}: no macro-block contains ({x}, {y})")
        if not _is_core_block(blocks[idx], core_polys):
            raise RuntimeError(
                f"{role}: block {idx} containing ({x}, {y}) is not a core block"
            )
        chosen.append(
            {
                "role": role,
                "block_id": idx,
                "point": [x, y],
                "area": round(float(blocks[idx].area), 4),
                "mass": round(float(density_field.mass(blocks[idx])), 4),
                "is_core": True,
            }
        )

    # Non-core band: primary point, else scan y≈63, x in 85..105 and take the
    # non-core hit with the highest density mass (ties -> lowest block id).
    px, py = _NONCORE_POINT
    idx = _block_containing(blocks, px, py)
    point_used = [px, py]
    if idx is None or _is_core_block(blocks[idx], core_polys):
        candidates: dict[int, list[float]] = {}
        for x in _NONCORE_SCAN_XS:
            hit = _block_containing(blocks, x, _NONCORE_SCAN_Y)
            if hit is None or _is_core_block(blocks[hit], core_polys):
                continue
            candidates.setdefault(hit, [x, _NONCORE_SCAN_Y])
        if not candidates:
            raise RuntimeError(
                f"noncore_band: no non-core block found along y={_NONCORE_SCAN_Y}, "
                f"x in {_NONCORE_SCAN_XS[0]}..{_NONCORE_SCAN_XS[-1]}"
            )
        scored = sorted(
            candidates,
            key=lambda i: (-float(density_field.mass(blocks[i])), i),
        )
        idx = scored[0]
        point_used = candidates[idx]
    chosen.append(
        {
            "role": "noncore_band",
            "block_id": idx,
            "point": point_used,
            "area": round(float(blocks[idx].area), 4),
            "mass": round(float(density_field.mass(blocks[idx])), 4),
            "is_core": False,
        }
    )
    return chosen


# ---------------------------------------------------------------------------
# Per-cell Chen run (mirrors r1_seam.chen_in_block's retry ladder, but with the
# knobs this ablation needs exposed)
# ---------------------------------------------------------------------------


def _config_kwargs(
    key: str,
    *,
    fields: IslandFields,
    global_m: float,
    min_parcel_area: float,
    n_from_a: int | None,
) -> dict[str, Any]:
    """generate_layout_for_boundary kwargs for one config (minus seed/boundary)."""
    density_field = build_density_field(fields)
    guidance_full = build_terrain_guidance(
        fields,
        strength=_HYBRID_GUIDANCE_STRENGTH,
        density_ridge_boost=_HYBRID_DENSITY_RIDGE_BOOST,
    )
    kwargs: dict[str, Any] = {
        "min_parcel_area": min_parcel_area,
        "split_weights": REGIONAL_SPLIT_WEIGHTS,
        "guidance": guidance_full,
        "density_field": density_field,
        "max_parcel_mass": global_m,
        "street_config": StreetConfig(avoid_cul_de_sacs=True),
    }
    if key == "A":
        pass
    elif key == "B":
        kwargs["split_weights"] = None  # paper defaults 0.30/0.50/0.20
    elif key == "C":
        kwargs["guidance"] = None
    elif key == "D":
        if n_from_a is None or n_from_a < 1:
            raise ValueError("config D needs A's district count")
        kwargs["density_field"] = None
        kwargs["max_parcel_mass"] = None
        kwargs["min_parcel_area"] = None
        kwargs["parcel_count"] = n_from_a
    elif key == "E":
        kwargs["guidance"] = build_terrain_guidance(
            fields,
            strength=_HYBRID_GUIDANCE_STRENGTH * 0.5,
            density_ridge_boost=_HYBRID_DENSITY_RIDGE_BOOST * 0.5,
        )
    elif key == "F":
        kwargs["split_weights"] = None
        kwargs["guidance"] = None
    else:
        raise ValueError(f"unknown config {key!r}")
    return kwargs


def run_cell(
    block: sg.Polygon,
    kwargs: dict[str, Any],
    *,
    seeds: tuple[int, ...] = DEFAULT_RETRY_SEEDS,
    timeout_s: float = 180.0,
) -> dict[str, Any]:
    """One (block, config) cell: seed retry ladder under a wall-clock budget.

    Returns {status, seed_used, seconds, districts, streets, irregularity_avg}.
    status is "ok", "timeout" (budget exceeded mid-ladder), or "all_failed".
    Never raises for a Chen failure.
    """
    spec = BoundarySpec(name="fan_probe_block", geom=block)
    t0 = time.perf_counter()
    old = signal.signal(signal.SIGALRM, _alarm)
    signal.setitimer(signal.ITIMER_REAL, timeout_s)
    try:
        for seed in seeds:
            try:
                generated = generate_layout_for_boundary(spec, seed=seed, **kwargs)
            except _CellTimeout:
                raise
            except Exception:  # noqa: BLE001 — per-seed failure, try next seed
                continue
            districts = [p.geom for p in generated.layout.mesh.parcels.values()]
            streets = [line for line, _props in _street_lines(generated.layout)]
            return {
                "status": "ok",
                "seed_used": seed,
                "seconds": round(time.perf_counter() - t0, 2),
                "districts": districts,
                "streets": streets,
                "irregularity_avg": generated.metrics.get("irregularity_avg"),
            }
        status = "all_failed"
    except _CellTimeout:
        status = "timeout"
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, old)
    return {
        "status": status,
        "seed_used": None,
        "seconds": round(time.perf_counter() - t0, 2),
        "districts": [],
        "streets": [],
        "irregularity_avg": None,
    }


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def elongation(poly: sg.Polygon) -> float:
    """Long/short side ratio of the minimum rotated rectangle (>= 1)."""
    mrr = poly.minimum_rotated_rectangle
    if mrr.geom_type != "Polygon":
        return float("inf")  # degenerate parcel collapses to a line/point
    coords = list(mrr.exterior.coords)
    s1 = float(np.hypot(coords[1][0] - coords[0][0], coords[1][1] - coords[0][1]))
    s2 = float(np.hypot(coords[2][0] - coords[1][0], coords[2][1] - coords[1][1]))
    lo, hi = sorted((s1, s2))
    if lo <= 1e-9:
        return float("inf")
    return hi / lo


def cell_metrics(cell: dict[str, Any]) -> dict[str, Any]:
    """Elongation/count summary for one cell result (JSON-safe)."""
    districts: list[sg.Polygon] = cell["districts"]
    n = len(districts)
    out: dict[str, Any] = {
        "status": cell["status"],
        "seed_used": cell["seed_used"],
        "seconds": cell["seconds"],
        "n_districts": n,
        "irregularity_avg": (
            round(float(cell["irregularity_avg"]), 4)
            if cell["irregularity_avg"] is not None
            else None
        ),
    }
    if n == 0:
        out.update(
            {
                "elong_median": None,
                "elong_max": None,
                "share_elong_gt4": None,
                "share_elong_gt6": None,
            }
        )
        return out
    elongs = np.array([elongation(p) for p in districts], dtype=float)
    finite = elongs[np.isfinite(elongs)]
    emax = float(finite.max()) if finite.size else float("inf")
    out.update(
        {
            "elong_median": round(float(np.median(elongs)), 2),
            "elong_max": round(emax, 2) if np.isfinite(emax) else "inf",
            "share_elong_gt4": round(float(np.mean(elongs > 4.0)), 3),
            "share_elong_gt6": round(float(np.mean(elongs > 6.0)), 3),
        }
    )
    return out


def print_table(blocks: list[dict[str, Any]], table: dict[str, dict[str, Any]]) -> None:
    """Compact (blocks x configs) metrics table on stdout."""
    header = (
        f"{'block':<22}{'cfg':<5}{'status':<11}{'n':>5}{'med':>7}{'max':>8}"
        f"{'>4':>7}{'>6':>7}{'irr':>8}{'sec':>8}"
    )
    print("\n" + header)
    print("-" * len(header))
    for binfo in blocks:
        tag = f"{binfo['role']}#{binfo['block_id']}"
        for key, _label in _CONFIGS:
            m = table[tag][key]
            fmt = lambda v, spec: "-" if v is None else format(v, spec)  # noqa: E731
            print(
                f"{tag:<22}{key:<5}{m['status']:<11}{m['n_districts']:>5}"
                f"{fmt(m['elong_median'], '.2f'):>7}"
                f"{str(m['elong_max']) if m['elong_max'] is not None else '-':>8}"
                f"{fmt(m['share_elong_gt4'], '.0%'):>7}"
                f"{fmt(m['share_elong_gt6'], '.0%'):>7}"
                f"{fmt(m['irregularity_avg'], '.4f'):>8}"
                f"{m['seconds']:>8.1f}"
            )
        print("-" * len(header))


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


def render_block_matrix(
    *,
    fields: IslandFields,
    block: sg.Polygon,
    binfo: dict[str, Any],
    cells: dict[str, dict[str, Any]],
    metrics: dict[str, dict[str, Any]],
    out_path: Path,
    dpi: int,
) -> None:
    """One contact sheet: the 6 configs as subplots, tight to the block bounds."""
    density = fields.density
    nrows, ncols = density.shape
    minx, miny, maxx, maxy = block.bounds
    pad = 0.06 * max(maxx - minx, maxy - miny)

    fig, axes = plt.subplots(2, 3, figsize=(16, 10.5))
    for ax, (key, label) in zip(axes.flat, _CONFIGS, strict=True):
        _render_density_backdrop(
            ax,
            density,
            fields.height_carved,
            fields.x0,
            fields.y0,
            fields.cell,
            nrows,
            ncols,
        )
        # Block outline for reference (orange, under the fabric).
        bx, by = block.exterior.xy
        ax.plot(bx, by, color="#d35400", lw=1.4, alpha=0.9, zorder=2)

        cell = cells[key]
        for poly in cell["districts"]:
            if poly.is_empty:
                continue
            for ring in [poly.exterior, *list(poly.interiors)]:
                rx, ry = ring.xy
                ax.plot(rx, ry, color="#1a1a1a", lw=0.6, alpha=0.95, zorder=4)
        for line in cell["streets"]:
            if line.geom_type != "LineString":
                continue
            lx, ly = line.xy
            ax.plot(lx, ly, color="#1f5fb0", lw=0.35, alpha=0.6, zorder=3)
        if cell["status"] != "ok":
            ax.text(
                0.5,
                0.5,
                cell["status"].upper(),
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=18,
                color="#b30000",
            )

        m = metrics[key]
        stats = (
            f"n={m['n_districts']}  med={m['elong_median']}  "
            f"max={m['elong_max']}  >4:{m['share_elong_gt4']}  "
            f">6:{m['share_elong_gt6']}"
            if m["status"] == "ok"
            else m["status"]
        )
        ax.set_title(f"{key}: {label}\n{stats}", fontsize=9)
        ax.set_xlim(minx - pad, maxx + pad)
        ax.set_ylim(miny - pad, maxy + pad)
        ax.set_aspect("equal")
        ax.set_xticks([])
        ax.set_yticks([])

    fig.suptitle(
        f"fan probe — {binfo['role']} (block {binfo['block_id']}, "
        f"area {binfo['area']:.1f}, mass {binfo['mass']:.1f})",
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def run_probe(
    in_dir: Path,
    out_dir: Path,
    *,
    total_target: int,
    timeout_s: float,
    dpi: int,
) -> dict[str, Any]:
    t_start = time.perf_counter()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Building stage-1 macro layer (shared build_macro_layer)…", flush=True)
    fields = IslandFields.from_npz(str(in_dir / "fields.npz"))
    boundary = load_boundary(in_dir)
    points = pl.read_parquet(in_dir / "island_points.parquet")
    layer = build_macro_layer(fields, boundary, points, MacroParams())
    print(
        f"  {len(layer.blocks)} macro-blocks, {len(layer.core_polys)} core rings",
        flush=True,
    )

    # Global calibration, exactly as run_r1_hybrid (LOCKED design decision).
    density_field = build_density_field(fields)
    total_mass = float(density_field.mass(boundary))
    island_area = float(boundary.area)
    global_m = total_mass / float(total_target)
    min_parcel_area = island_area / (float(total_target) * 4.0)
    print(
        f"  calibration: total_mass={total_mass:.2f}, M={global_m:.4f}, "
        f"min_parcel_area={min_parcel_area:.4f}",
        flush=True,
    )

    blocks_info = select_probe_blocks(layer.blocks, layer.core_polys, density_field)
    for b in blocks_info:
        print(
            f"  probe block: {b['role']} -> block {b['block_id']} "
            f"(point {b['point']}, area {b['area']}, mass {b['mass']}, "
            f"core={b['is_core']})",
            flush=True,
        )

    table: dict[str, dict[str, Any]] = {}
    render_paths: list[str] = []
    for binfo in blocks_info:
        tag = f"{binfo['role']}#{binfo['block_id']}"
        block = layer.blocks[binfo["block_id"]]
        cells: dict[str, dict[str, Any]] = {}
        metrics: dict[str, dict[str, Any]] = {}
        n_from_a: int | None = None
        for key, label in _CONFIGS:
            if key == "D" and (n_from_a is None or n_from_a < 1):
                print(f"  {tag} {key}: skipped (A produced no districts)", flush=True)
                metrics[key] = cell_metrics(
                    {
                        "status": "skipped_no_A",
                        "seed_used": None,
                        "seconds": 0.0,
                        "districts": [],
                        "streets": [],
                        "irregularity_avg": None,
                    }
                )
                cells[key] = {"status": "skipped_no_A", "districts": [], "streets": []}
                continue
            kwargs = _config_kwargs(
                key,
                fields=fields,
                global_m=global_m,
                min_parcel_area=min_parcel_area,
                n_from_a=n_from_a,
            )
            print(f"  {tag} {key} ({label})…", flush=True)
            cell = run_cell(block, kwargs, timeout_s=timeout_s)
            if key == "A" and cell["status"] == "ok":
                n_from_a = len(cell["districts"])
            cells[key] = cell
            metrics[key] = cell_metrics(cell)
            print(
                f"    -> {cell['status']} n={len(cell['districts'])} "
                f"{cell['seconds']}s",
                flush=True,
            )
        table[tag] = metrics

        out_png = out_dir / f"block{binfo['block_id']}_matrix.png"
        render_block_matrix(
            fields=fields,
            block=block,
            binfo=binfo,
            cells=cells,
            metrics=metrics,
            out_path=out_png,
            dpi=dpi,
        )
        render_paths.append(str(out_png))
        print(f"  wrote {out_png}", flush=True)

    print_table(blocks_info, table)

    result: dict[str, Any] = {
        "probe": "r2_fan_probe (one-knob ablation of the summit sliver-fan)",
        "configs": {key: label for key, label in _CONFIGS},
        "calibration": {
            "total_target": total_target,
            "M": round(global_m, 6),
            "min_parcel_area": round(min_parcel_area, 6),
            "total_mass": round(total_mass, 4),
            "island_area": round(island_area, 4),
            "guidance_strength": _HYBRID_GUIDANCE_STRENGTH,
            "density_ridge_boost": _HYBRID_DENSITY_RIDGE_BOOST,
            "split_weights_control": {
                "size": REGIONAL_SPLIT_WEIGHTS.size,
                "regularity": REGIONAL_SPLIT_WEIGHTS.regularity,
                "access": REGIONAL_SPLIT_WEIGHTS.access,
            },
            "retry_seeds": list(DEFAULT_RETRY_SEEDS),
            "cell_timeout_seconds": timeout_s,
        },
        "blocks": blocks_info,
        "metrics": table,
        "renders": [Path(p).name for p in render_paths],
        "runtime_seconds": round(time.perf_counter() - t_start, 2),
    }
    with (out_dir / "fan_probe_metrics.json").open("w") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    print(f"\nWrote {out_dir / 'fan_probe_metrics.json'}", flush=True)
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R2 fan probe: one-knob ablation of the summit sliver-fan"
    )
    parser.add_argument("--in-dir", type=Path, default=_DEFAULT_IN)
    parser.add_argument("--out-dir", type=Path, default=_DEFAULT_OUT)
    parser.add_argument(
        "--total-target",
        type=int,
        default=1200,
        help="Island-wide district target; sets global M (matches run_r1_hybrid)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="Per-(block, config) wall-clock budget in seconds",
    )
    parser.add_argument("--dpi", type=int, default=150)
    args = parser.parse_args(argv)

    result = run_probe(
        args.in_dir,
        args.out_dir,
        total_target=args.total_target,
        timeout_s=args.timeout,
        dpi=args.dpi,
    )
    print(f"Done in {result['runtime_seconds']}s. Outputs in: {args.out_dir}")


if __name__ == "__main__":
    main()
