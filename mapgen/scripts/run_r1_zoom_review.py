#!/usr/bin/env python
"""Chen/R2 zoom-in visual validation harness.

Generates finer density-mass (R2) Chen layouts on the staged island and renders
a phone-reviewable image set: a full-island overview plus zoom-ins into the
densest cores, each overlaying the density backdrop, district polygons,
arterials, and the actual world points coloured by l0 cluster (with the dominant
cluster name annotated on the largest districts in view). A ``scale_compare``
figure renders one shared dense window across all target scales.

This is a review harness; it does not modify any ``chen_*`` source and reuses the
R2 generation path and the ``r1_compare`` matplotlib primitives. See
``docs/regional-2_5d-research.md`` (R2).

  uv run python scripts/run_r1_zoom_review.py --targets 120,360
"""

from __future__ import annotations

import argparse
import json
import time
import traceback
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import shapely  # noqa: E402
from shapely import LineString, Polygon  # noqa: E402

from mapgen.chen_artifacts import _street_lines  # noqa: E402
from mapgen.chen_generate import (  # noqa: E402
    BoundarySpec,
    GeneratedChenLayout,
    generate_layout_for_boundary,
)
from mapgen.chen_streets import StreetConfig  # noqa: E402
from mapgen.r1_arm_a import (  # noqa: E402
    REGIONAL_SPLIT_WEIGHTS,
    IslandFields,
    build_density_field,
    build_terrain_guidance,
    default_max_parcel_mass,
)
from mapgen.r1_arm_b import find_density_peaks  # noqa: E402
from mapgen.r1_compare import (  # noqa: E402
    _render_arterials,
    _render_boundary,
    _render_density_backdrop,
    _render_districts,
    _set_extent,
)
from mapgen.r1_zoom import (  # noqa: E402
    LabeledPoints,
    dominant_cluster_per_district,
    load_points_with_labels,
    zoom_window,
)

DEFAULT_INPUTS = Path("artifacts/r1/inputs")
DEFAULT_OUT = Path("artifacts/r1/zoom_review")


def load_boundary(inputs_dir: Path) -> BoundarySpec:
    raw = json.loads((inputs_dir / "island_boundary.geojson").read_text())
    if raw.get("type") == "FeatureCollection":
        geometry = raw["features"][0]["geometry"]
    elif raw.get("type") == "Feature":
        geometry = raw["geometry"]
    else:
        geometry = raw
    geom = shapely.geometry.shape(geometry)
    if not isinstance(geom, Polygon):
        raise ValueError(f"island boundary must be a Polygon, got {geom.geom_type}")
    return BoundarySpec(name="island", geom=geom)


def _district_polys(generated: GeneratedChenLayout) -> list[Polygon]:
    return [parcel.geom for parcel in generated.layout.mesh.parcels.values()]


def _arterial_lines(generated: GeneratedChenLayout) -> list[LineString]:
    return [line for line, _props in _street_lines(generated.layout)]


def generate_layout(
    *,
    boundary: BoundarySpec,
    fields: IslandFields,
    target: int,
    floor_factor: float,
    seed: int,
) -> tuple[GeneratedChenLayout | None, dict[str, Any]]:
    """Generate one R2 density-mass layout calibrated to ``target`` districts.

    ``target`` sets the per-district mass target; ``parcel_count`` (the geometric
    floor + streamline scale) is kept finer (``floor_factor * target``) so the
    mass gate governs. Returns ``(generated_or_None, info)``.
    """
    guidance = build_terrain_guidance(fields, strength=6.0, density_ridge_boost=2.0)
    density_field = build_density_field(fields)
    max_parcel_mass = default_max_parcel_mass(density_field, target)
    parcel_count = max(int(round(target * floor_factor)), target + 1)
    info: dict[str, Any] = {
        "target": target,
        "parcel_count_floor": parcel_count,
        "max_parcel_mass": round(float(max_parcel_mass), 4),
        "seed": seed,
    }
    start = time.perf_counter()
    try:
        generated = generate_layout_for_boundary(
            boundary,
            parcel_count=parcel_count,
            seed=seed,
            split_weights=REGIONAL_SPLIT_WEIGHTS,
            guidance=guidance,
            density_field=density_field,
            max_parcel_mass=max_parcel_mass,
            street_config=StreetConfig(avoid_cul_de_sacs=True),
        )
    except Exception as exc:  # noqa: BLE001
        info["status"] = "error"
        info["error"] = f"{type(exc).__name__}: {exc}"
        info["traceback"] = traceback.format_exc()
        info["seconds"] = round(time.perf_counter() - start, 2)
        return None, info
    info["status"] = "ok"
    info["seconds"] = round(time.perf_counter() - start, 2)
    info["district_count"] = len(generated.layout.mesh.parcels)
    info["geometry_valid_pass"] = bool(generated.metrics.get("geometry_valid_pass"))
    info["paper_invariant_pass"] = bool(generated.metrics.get("paper_invariant_pass"))
    return generated, info


def _color_lookup(l0_ids: np.ndarray) -> dict[int, tuple[float, float, float, float]]:
    unique = sorted({int(v) for v in l0_ids})
    cmap = plt.get_cmap("tab20")
    return {uid: cmap(i % 20) for i, uid in enumerate(unique)}


def _render_points(
    ax: Any,
    lp: LabeledPoints,
    colors: dict[int, tuple[float, float, float, float]],
    *,
    s: float,
    alpha: float,
) -> None:
    pt_colors = [colors[int(i)] for i in lp.l0_ids]
    ax.scatter(lp.xs, lp.ys, s=s, c=pt_colors, alpha=alpha, linewidths=0.0, zorder=5)


def _annotate_dominant(
    ax: Any,
    districts: list[Polygon],
    dom: dict[int, tuple[str, int, int]],
    window: tuple[float, float, float, float],
    *,
    max_labels: int = 6,
) -> None:
    minx, miny, maxx, maxy = window
    candidates: list[tuple[float, int]] = []
    for i, poly in enumerate(districts):
        if i not in dom:
            continue
        cx, cy = poly.centroid.x, poly.centroid.y
        if not (minx <= cx <= maxx and miny <= cy <= maxy):
            continue
        candidates.append((poly.area, i))
    candidates.sort(reverse=True)
    for _area, i in candidates[:max_labels]:
        name, count, total = dom[i]
        cx, cy = districts[i].centroid.x, districts[i].centroid.y
        label = name if len(name) <= 22 else name[:21] + "…"
        ax.text(
            cx,
            cy,
            f"{label}\n{count}/{total}",
            fontsize=6,
            ha="center",
            va="center",
            color="white",
            zorder=9,
            bbox={"boxstyle": "round,pad=0.15", "fc": "black", "alpha": 0.55, "lw": 0},
        )


def render_layout_images(
    *,
    generated: GeneratedChenLayout,
    fields: IslandFields,
    boundary: BoundarySpec,
    lp: LabeledPoints,
    colors: dict[int, tuple[float, float, float, float]],
    target: int,
    out_dir: Path,
    k_cores: int,
    dpi: int,
) -> list[Path]:
    density = fields.density
    nrows, ncols = density.shape
    x0, y0, cell = fields.x0, fields.y0, fields.cell
    districts = _district_polys(generated)
    arterials = _arterial_lines(generated)
    dom = dominant_cluster_per_district(districts, lp.xs, lp.ys, lp.l0_names)

    written: list[Path] = []

    # Overview.
    fig, ax = plt.subplots(figsize=(11, 8))
    _render_density_backdrop(
        ax, density, fields.height_carved, x0, y0, cell, nrows, ncols
    )
    _render_boundary(ax, boundary.geom)
    _render_districts(ax, districts, edge_color="#f5f5f5", fill_alpha=0.0, lw=1.0)
    _render_arterials(ax, arterials, color="#0b3d91", lw=1.0)
    _render_points(ax, lp, colors, s=2.0, alpha=0.3)
    _set_extent(ax, boundary.geom, pad=2.0)
    ax.set_title(
        f"R2 density-mass — target {target} "
        f"({len(districts)} districts, {len(arterials)} arterials)",
        fontsize=10,
    )
    fig.tight_layout()
    overview = out_dir / f"overview_t{target}.png"
    fig.savefig(overview, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    written.append(overview)

    # Dense-core zooms.
    peak_xs, peak_ys, peak_ds = find_density_peaks(density, x0, y0, cell)
    order = np.argsort(peak_ds)[::-1]
    span = max(
        boundary.geom.bounds[2] - boundary.geom.bounds[0],
        boundary.geom.bounds[3] - boundary.geom.bounds[1],
    )
    half_span = span * 0.12
    for rank, pidx in enumerate(order[:k_cores], start=1):
        cx, cy = float(peak_xs[pidx]), float(peak_ys[pidx])
        window = zoom_window(cx, cy, half_span)
        fig, ax = plt.subplots(figsize=(9, 9))
        _render_density_backdrop(
            ax, density, fields.height_carved, x0, y0, cell, nrows, ncols
        )
        _render_boundary(ax, boundary.geom)
        _render_districts(ax, districts, edge_color="#111111", fill_alpha=0.05, lw=1.0)
        _render_arterials(ax, arterials, color="#0b3d91", lw=1.6)
        _render_points(ax, lp, colors, s=14.0, alpha=0.85)
        _annotate_dominant(ax, districts, dom, window)
        ax.set_xlim(window[0], window[2])
        ax.set_ylim(window[1], window[3])
        ax.set_aspect("equal")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(
            f"dense core #{rank} @ ({cx:.0f},{cy:.0f}) — target {target}", fontsize=10
        )
        fig.tight_layout()
        core = out_dir / f"core{rank}_t{target}.png"
        fig.savefig(core, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        written.append(core)

    return written


def render_scale_compare(
    *,
    layouts: list[tuple[int, GeneratedChenLayout]],
    fields: IslandFields,
    boundary: BoundarySpec,
    lp: LabeledPoints,
    colors: dict[int, tuple[float, float, float, float]],
    out_dir: Path,
    dpi: int,
) -> Path | None:
    if not layouts:
        return None
    density = fields.density
    nrows, ncols = density.shape
    x0, y0, cell = fields.x0, fields.y0, fields.cell
    # Shared window: the single densest peak.
    peak_xs, peak_ys, peak_ds = find_density_peaks(density, x0, y0, cell)
    top = int(np.argmax(peak_ds))
    cx, cy = float(peak_xs[top]), float(peak_ys[top])
    span = max(
        boundary.geom.bounds[2] - boundary.geom.bounds[0],
        boundary.geom.bounds[3] - boundary.geom.bounds[1],
    )
    window = zoom_window(cx, cy, span * 0.12)

    n = len(layouts)
    fig, axes = plt.subplots(1, n, figsize=(7 * n, 7), squeeze=False)
    for ax, (target, generated) in zip(axes[0], layouts, strict=True):
        districts = _district_polys(generated)
        arterials = _arterial_lines(generated)
        _render_density_backdrop(
            ax, density, fields.height_carved, x0, y0, cell, nrows, ncols
        )
        _render_boundary(ax, boundary.geom)
        _render_districts(ax, districts, edge_color="#111111", fill_alpha=0.05, lw=1.0)
        _render_arterials(ax, arterials, color="#0b3d91", lw=1.6)
        _render_points(ax, lp, colors, s=12.0, alpha=0.85)
        ax.set_xlim(window[0], window[2])
        ax.set_ylim(window[1], window[3])
        ax.set_aspect("equal")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(f"target {target} ({len(districts)} districts)", fontsize=10)
    fig.suptitle("R2 density-mass — same dense window across scales", fontsize=12)
    fig.tight_layout()
    out = out_dir / "scale_compare.png"
    fig.savefig(out, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs-dir", type=Path, default=DEFAULT_INPUTS)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--targets", type=str, default="120,360")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--k-cores", type=int, default=4)
    parser.add_argument("--floor-factor", type=float, default=2.0)
    parser.add_argument("--dpi", type=int, default=220)
    args = parser.parse_args()

    targets = [int(t) for t in args.targets.split(",") if t.strip()]
    args.out_dir.mkdir(parents=True, exist_ok=True)

    boundary = load_boundary(args.inputs_dir)
    fields = IslandFields.from_npz(str(args.inputs_dir / "fields.npz"))
    lp = load_points_with_labels(str(args.inputs_dir / "island_points.parquet"))
    colors = _color_lookup(lp.l0_ids)
    print(f"[zoom] {len(lp)} world points, {len(colors)} l0 clusters", flush=True)

    run_infos: list[dict[str, Any]] = []
    ok_layouts: list[tuple[int, GeneratedChenLayout]] = []
    for target in targets:
        print(f"[zoom] generating target {target} ...", flush=True)
        generated, info = generate_layout(
            boundary=boundary,
            fields=fields,
            target=target,
            floor_factor=args.floor_factor,
            seed=args.seed,
        )
        run_infos.append(info)
        print(
            f"[zoom]   target {target}: {info['status']} "
            f"({info.get('district_count', '?')} districts, "
            f"{info.get('seconds', '?')}s)",
            flush=True,
        )
        if generated is None:
            continue
        written = render_layout_images(
            generated=generated,
            fields=fields,
            boundary=boundary,
            lp=lp,
            colors=colors,
            target=target,
            out_dir=args.out_dir,
            k_cores=args.k_cores,
            dpi=args.dpi,
        )
        info["images"] = [str(p) for p in written]
        ok_layouts.append((target, generated))

    compare = render_scale_compare(
        layouts=ok_layouts,
        fields=fields,
        boundary=boundary,
        lp=lp,
        colors=colors,
        out_dir=args.out_dir,
        dpi=args.dpi,
    )

    manifest = {
        "inputs_dir": str(args.inputs_dir),
        "targets": targets,
        "seed": args.seed,
        "floor_factor": args.floor_factor,
        "runs": run_infos,
        "scale_compare": str(compare) if compare else None,
    }
    (args.out_dir / "zoom_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    print(f"[zoom] wrote outputs to {args.out_dir}", flush=True)
    print(json.dumps({k: manifest[k] for k in ("targets", "runs")}, indent=2))


if __name__ == "__main__":
    main()
