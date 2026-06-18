#!/usr/bin/env python
"""R1 probe — cross-arm comparison metrics and side-by-side renders.

Compares Arm A (4 configs), Arm B, and the KMeans l0 districts derived from
island_points.parquet.  Outputs metrics as JSON + Markdown and renders two
PNG composite images.

Outputs to mapgen/artifacts/r1/compare/ (or --out-dir):
  comparison.json     — full per-layout metrics
  comparison.md       — readable Markdown table
  compare_main.png    — 3-panel: Arm A regional_strong | Arm B | KMeans
  compare_contact.png — 6-panel: all 4 Arm A configs + Arm B + KMeans

Run from mapgen/::
    uv run python scripts/run_r1_compare.py

Or with custom paths::
    uv run python scripts/run_r1_compare.py \\
        --in-dir artifacts/r1/inputs \\
        --arm-a-dir artifacts/r1/arm_a \\
        --arm-b-dir artifacts/r1/arm_b \\
        --out-dir artifacts/r1/compare
"""

from __future__ import annotations

import argparse
from pathlib import Path

_DEFAULT_IN = Path(__file__).resolve().parents[1] / "artifacts/r1/inputs"
_DEFAULT_ARM_A = Path(__file__).resolve().parents[1] / "artifacts/r1/arm_a"
_DEFAULT_ARM_B = Path(__file__).resolve().parents[1] / "artifacts/r1/arm_b"
_DEFAULT_OUT = Path(__file__).resolve().parents[1] / "artifacts/r1/compare"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R1 cross-arm comparison: metrics + renders"
    )
    parser.add_argument(
        "--in-dir",
        type=Path,
        default=_DEFAULT_IN,
        help="Input directory with fields.npz, island_boundary.geojson, "
        "island_points.parquet (default: artifacts/r1/inputs)",
    )
    parser.add_argument(
        "--arm-a-dir",
        type=Path,
        default=_DEFAULT_ARM_A,
        help="Arm A directory containing config subdirectories "
        "(default: artifacts/r1/arm_a)",
    )
    parser.add_argument(
        "--arm-b-dir",
        type=Path,
        default=_DEFAULT_ARM_B,
        help="Arm B directory (default: artifacts/r1/arm_b)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_DEFAULT_OUT,
        help="Output directory (default: artifacts/r1/compare)",
    )
    args = parser.parse_args(argv)

    from mapgen.r1_compare import run_compare

    comparison = run_compare(
        in_dir=args.in_dir,
        arm_a_dir=args.arm_a_dir,
        arm_b_dir=args.arm_b_dir,
        out_dir=args.out_dir,
    )

    # Print summary
    print("\n--- R1 Compare Summary ---")
    isle = comparison["island_summary"]
    print(
        f"  Island: {isle['total_worlds']} worlds, "
        f"{isle['l0_clusters_on_island']} l0 clusters, "
        f"{isle['l0_clusters_ge50_worlds']} with ≥50 worlds"
    )

    thr = comparison["thresholds"]
    print(f"  Slope p50 threshold: {thr['slope_p50_inside_mask']:.6f}")
    river_thr = thr["river_flow_accum_p99_inside_mask"]
    print(f"  River flow_accum p99 threshold: {river_thr:.1f}")

    print("\n  Layout metrics:")
    for lay in comparison["layouts"]:
        da = lay["district_area"]
        dc = lay["density_area_corr"]
        am = lay["arterials"]
        art_str = ""
        if isinstance(am, dict):
            art_str = (
                f"  art_len={am['total_length']:.1f}"
                f"  terrain_frac={am.get('terrain_alignment_frac', '?')}"
                f"  river_x={am.get('river_crossings', '?')}"
            )
        print(
            f"  [{lay['layout']:30s}]  n={lay['district_count']:3d}"
            f"  area_med={da['median']:7.1f}"
            f"  area_cv={da['cv']:.3f}"
            f"  dens_area_r={dc['spearman_r']}"
            f"  wc_cv={lay['world_count_balance_cv']:.3f}"
            f"  cvx_med={lay['shape']['convexity_median']:.3f}" + art_str
        )

    if comparison["anomalies"]:
        print("\n  Anomalies:")
        for a in comparison["anomalies"]:
            print(f"    - {a}")
    else:
        print("\n  No world-count anomalies detected.")

    print(f"\nOutputs in: {args.out_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
