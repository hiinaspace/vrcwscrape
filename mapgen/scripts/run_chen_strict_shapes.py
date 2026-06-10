#!/usr/bin/env python
"""Run strict Chen generation for the standard visual-check shapes."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mapgen.chen_artifacts import write_strict_chen_artifacts

SHAPES = ("square", "oval", "triangle")
STREAMLINE_MODES = (
    "baseline",
    "yang_d_field_candidates",
    "yang_b_field_candidates",
)


def _load_generate_named_layout() -> Callable[..., Any]:
    try:
        from mapgen.chen_generate import generate_named_layout
    except ModuleNotFoundError as exc:
        if exc.name == "mapgen.chen_generate":
            raise SystemExit(
                "mapgen.chen_generate is not available yet; run this after the "
                "strict Chen generation slice lands."
            ) from exc
        raise
    return generate_named_layout


def run_shapes(
    *,
    out_dir: Path,
    parcel_count: int,
    seed: int,
    width: float,
    height: float,
    streamline_mode: str = "baseline",
) -> dict[str, Any]:
    generate_named_layout = _load_generate_named_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "layout": "chen-strict-shape-suite",
        "shapes": [],
        "parameters": {
            "parcel_count": parcel_count,
            "seed": seed,
            "width": width,
            "height": height,
            "streamline_mode": streamline_mode,
        },
        "limitations": _limitations(streamline_mode),
    }
    for index, name in enumerate(SHAPES):
        generated = generate_named_layout(
            name,
            parcel_count=parcel_count,
            seed=seed + index,
            width=width,
            height=height,
            streamline_mode=streamline_mode,
        )
        shape_dir = out_dir / name
        artifacts = write_strict_chen_artifacts(generated, shape_dir)
        manifest["shapes"].append(
            {
                "name": name,
                "seed": seed + index,
                "dir": shape_dir.name,
                "files": artifacts["files"],
                "summary": artifacts["summary"],
            }
        )
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write strict Chen square/oval/triangle visual artifacts."
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("artifacts/chen_strict_shapes"),
        help="output directory for shape subdirectories",
    )
    parser.add_argument("--parcel-count", type=int, default=48)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--width", type=float, default=180.0)
    parser.add_argument("--height", type=float, default=140.0)
    parser.add_argument(
        "--streamline-mode",
        choices=STREAMLINE_MODES,
        default="baseline",
        help=(
            "streamline candidate mode for non-rectangular shapes; "
            "yang_d_field_candidates and yang_b_field_candidates enable opt-in "
            "Yang mesh seed and DIV/DB/DS/CT score paths"
        ),
    )
    return parser.parse_args()


def _limitations(streamline_mode: str) -> list[str]:
    limitations = [
        "grid_smooth_4rosy_laplace_v1_not_full_yang_global_solver",
        "bounded_junction_street_selection_v0_not_full_section_4_2_solver",
        "shapeop_like_projection_v1_not_exact_shapeop_solver",
    ]
    if streamline_mode == "yang_d_field_candidates":
        limitations.append(
            "yang_d_field_candidates_mode_is_opt_in_and_still_approximate"
        )
    if streamline_mode == "yang_b_field_candidates":
        limitations.append(
            "yang_b_field_candidates_mode_is_opt_in_uniform_clipped_mesh_"
            "laplacian_omega_boundary_alignment_and_still_approximate"
        )
    return limitations


def main() -> None:
    args = parse_args()
    manifest = run_shapes(
        out_dir=args.out_dir,
        parcel_count=args.parcel_count,
        seed=args.seed,
        width=args.width,
        height=args.height,
        streamline_mode=args.streamline_mode,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
