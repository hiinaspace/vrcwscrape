#!/usr/bin/env python
"""R1 probe — Arm B: v3-style least-cost arterial baseline.

Galin et al. 2010-style: least-cost arterial roads between density peaks over
a terrain/density cost field, then polygonize the arterial network into
districts.

Outputs to mapgen/artifacts/r1/arm_b/ (or --out-dir):
  peaks.geojson       — density peak locations
  arterials.geojson   — arterial LineStrings with cost/length properties
  districts.geojson   — district Polygons with id, area, world_count
  arm_b.png           — eyeballable render
  arm_b_manifest.json — parameters, counts, and area stats

Run from mapgen/::
    uv run python scripts/run_r1_arm_b_baseline.py

Or with custom options::
    uv run python scripts/run_r1_arm_b_baseline.py \\
        --in-dir artifacts/r1/inputs \\
        --out-dir artifacts/r1/arm_b \\
        --peak-min-distance 10.0 \\
        --peak-threshold-frac 0.03 \\
        --w-slope 8.0 \\
        --w-river 6.0 \\
        --beta-ratio 2.5
"""

from __future__ import annotations

import argparse
from pathlib import Path

_DEFAULT_IN = Path(__file__).resolve().parents[1] / "artifacts/r1/inputs"
_DEFAULT_OUT = Path(__file__).resolve().parents[1] / "artifacts/r1/arm_b"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R1 Arm B: least-cost arterial baseline"
    )
    parser.add_argument(
        "--in-dir",
        type=Path,
        default=_DEFAULT_IN,
        help="Input directory (default: artifacts/r1/inputs)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_DEFAULT_OUT,
        help="Output directory (default: artifacts/r1/arm_b)",
    )
    parser.add_argument(
        "--peak-min-distance",
        type=float,
        default=10.0,
        help="Minimum separation between peaks in island-frame units (default: 10.0). "
        "~10 units ≈ 20 raster cells at current resolution.",
    )
    parser.add_argument(
        "--peak-threshold-frac",
        type=float,
        default=0.03,
        help="Density threshold = frac * density.max() (default: 0.03)",
    )
    parser.add_argument(
        "--cost-base",
        type=float,
        default=1.0,
        help="Baseline cost per cell (default: 1.0)",
    )
    parser.add_argument(
        "--w-slope",
        type=float,
        default=8.0,
        help="Weight for normalized slope in cost field (default: 8.0)",
    )
    parser.add_argument(
        "--w-river",
        type=float,
        default=6.0,
        help="Weight for river-crossing penalty (flow_accum > p99, default: 6.0)",
    )
    parser.add_argument(
        "--beta-ratio",
        type=float,
        default=1.2,
        help="Pruning threshold: drop Delaunay edge if alternative path cost "
        "<= ratio * direct cost (default: 1.2; use inf to keep all edges)",
    )
    parser.add_argument(
        "--simplify-tolerance",
        type=float,
        default=1.5,
        help="Douglas-Peucker tolerance for arterial simplification, island units "
        "(default: 1.5)",
    )
    parser.add_argument(
        "--min-district-area",
        type=float,
        default=20.0,
        help="Minimum district area (island units²); smaller polygons are merged "
        "into a neighbor (default: 20.0)",
    )
    args = parser.parse_args(argv)

    from mapgen.r1_arm_b import run_arm_b

    manifest = run_arm_b(
        args.in_dir,
        args.out_dir,
        peak_min_distance_units=args.peak_min_distance,
        peak_threshold_frac=args.peak_threshold_frac,
        cost_base=args.cost_base,
        w_slope=args.w_slope,
        w_river=args.w_river,
        beta_ratio=args.beta_ratio,
        simplify_tolerance=args.simplify_tolerance,
        min_district_area=args.min_district_area,
    )

    # Summary
    r = manifest["results"]
    p = manifest["params"]
    print("\n--- Arm B summary ---")
    print(f"  peaks:      {r['n_peaks']}")
    print(f"  arterials:  {r['n_arterials']}")
    print(f"  total len:  {r['total_arterial_length']:.1f} island units")
    print(f"  districts:  {r['n_districts']}")
    print(f"  area stats: {r['district_area_stats']}")
    print(
        f"  cost weights: base={p['cost_base']}, "
        f"slope={p['w_slope']}, river={p['w_river']}"
    )
    print(f"  world counts: {r['district_world_counts']}")
    print(f"\nOutputs in: {args.out_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
