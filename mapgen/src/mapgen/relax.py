"""Relax 2D map coordinates to reduce deep-zoom degeneracy.

This is a local postprocess for UMAP/PacMAP coordinates. It only repels points
inside a target minimum distance and adds a weak spring back to the original
coordinates, so dense duplicate-like piles open up without turning the map into a
grid or re-solving the global layout.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np
import polars as pl
from scipy.spatial import cKDTree


def nearest_neighbor_distances(xy: np.ndarray) -> np.ndarray:
    tree = cKDTree(xy)
    d, _ = tree.query(xy, k=2, workers=-1)
    return d[:, 1]


def distance_summary(xy: np.ndarray) -> dict[str, float]:
    nn = nearest_neighbor_distances(xy)
    qs = {
        f"nn_q{int(q * 100):02d}": float(np.quantile(nn, q))
        for q in (0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)
    }
    qs["nn_min"] = float(nn.min())
    return qs


def _format_summary(label: str, stats: dict[str, float]) -> str:
    return (
        f"{label}: min={stats['nn_min']:.6g} "
        f"q01={stats['nn_q01']:.6g} q10={stats['nn_q10']:.6g} "
        f"q50={stats['nn_q50']:.6g} q90={stats['nn_q90']:.6g} "
        f"q99={stats['nn_q99']:.6g}"
    )


def _auto_min_dist(
    xy: np.ndarray, quantile: float, scale: float
) -> tuple[float, dict[str, float]]:
    stats = distance_summary(xy)
    key = f"nn_q{int(quantile * 100):02d}"
    if key in stats:
        base = stats[key]
    else:
        base = float(np.quantile(nearest_neighbor_distances(xy), quantile))
    return base * scale, stats


def relax_xy(
    xy: np.ndarray,
    *,
    min_dist: float | None = None,
    min_dist_quantile: float = 0.5,
    min_dist_scale: float = 2.0,
    iterations: int = 40,
    strength: float = 0.45,
    anchor: float = 0.015,
    max_step_scale: float = 0.8,
    max_displacement_scale: float = 20.0,
    seed: int = 0,
    verbose: bool = True,
) -> tuple[np.ndarray, dict[str, float]]:
    """Return relaxed coordinates and a stats dictionary."""
    orig = xy.astype("float64", copy=True)
    pos = orig.copy()
    if min_dist is None:
        min_dist, before = _auto_min_dist(pos, min_dist_quantile, min_dist_scale)
    else:
        before = distance_summary(pos)
    min_dist = float(min_dist)
    if min_dist <= 0:
        raise ValueError("min_dist must be positive")

    rng = np.random.default_rng(seed)
    max_step = min_dist * max_step_scale
    max_displacement = min_dist * max_displacement_scale
    eps = max(min_dist * 1e-6, 1e-12)
    if verbose:
        print(_format_summary("before", before))
        print(
            f"relax: min_dist={min_dist:.6g} iterations={iterations} "
            f"strength={strength} anchor={anchor}"
        )

    last_pairs = 0
    t0 = time.time()
    for it in range(iterations):
        pairs = cKDTree(pos).query_pairs(min_dist, output_type="ndarray")
        last_pairs = int(len(pairs))
        if last_pairs == 0:
            if verbose:
                print(f"  iter {it + 1}: no close pairs")
            break
        i = pairs[:, 0]
        j = pairs[:, 1]
        delta = pos[i] - pos[j]
        dist = np.linalg.norm(delta, axis=1)
        zero = dist < eps
        if zero.any():
            angle = rng.uniform(0.0, 2.0 * np.pi, size=int(zero.sum()))
            delta[zero, 0] = np.cos(angle) * eps
            delta[zero, 1] = np.sin(angle) * eps
            dist[zero] = eps

        overlap = min_dist - dist
        move = delta * ((overlap * strength * 0.5) / dist)[:, None]
        disp = np.zeros_like(pos)
        np.add.at(disp, i, move)
        np.add.at(disp, j, -move)

        step = np.linalg.norm(disp, axis=1)
        too_far = step > max_step
        if too_far.any():
            disp[too_far] *= (max_step / step[too_far])[:, None]
        pos += disp
        if anchor:
            pos += (orig - pos) * anchor
        if max_displacement_scale > 0:
            offset = pos - orig
            off_len = np.linalg.norm(offset, axis=1)
            clipped = off_len > max_displacement
            if clipped.any():
                pos[clipped] = orig[clipped] + offset[clipped] * (
                    max_displacement / off_len[clipped]
                )[:, None]

        if verbose and ((it + 1) % 10 == 0 or it == iterations - 1):
            print(f"  iter {it + 1}: close_pairs={last_pairs:,}")

    after = distance_summary(pos)
    disp = np.linalg.norm(pos - orig, axis=1)
    stats = {
        **{f"before_{k}": v for k, v in before.items()},
        **{f"after_{k}": v for k, v in after.items()},
        "min_dist": min_dist,
        "iterations": float(iterations),
        "last_close_pairs": float(last_pairs),
        "max_displacement": float(disp.max()),
        "median_displacement": float(np.median(disp)),
        "elapsed_s": time.time() - t0,
    }
    if verbose:
        print(_format_summary("after ", after))
        print(
            f"displacement: median={stats['median_displacement']:.6g} "
            f"max={stats['max_displacement']:.6g}; elapsed={stats['elapsed_s']:.1f}s"
        )
    return pos.astype("float32"), stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--coords", type=Path, required=True, help="parquet with x,y")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--min-dist", type=float, default=None)
    ap.add_argument("--min-dist-quantile", type=float, default=0.5)
    ap.add_argument("--min-dist-scale", type=float, default=2.0)
    ap.add_argument("--iterations", type=int, default=40)
    ap.add_argument("--strength", type=float, default=0.45)
    ap.add_argument("--anchor", type=float, default=0.015)
    ap.add_argument("--max-step-scale", type=float, default=0.8)
    ap.add_argument("--max-displacement-scale", type=float, default=20.0)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    df = pl.read_parquet(args.coords)
    xy = df.select("x", "y").to_numpy().astype("float64")
    relaxed, _stats = relax_xy(
        xy,
        min_dist=args.min_dist,
        min_dist_quantile=args.min_dist_quantile,
        min_dist_scale=args.min_dist_scale,
        iterations=args.iterations,
        strength=args.strength,
        anchor=args.anchor,
        max_step_scale=args.max_step_scale,
        max_displacement_scale=args.max_displacement_scale,
        seed=args.seed,
    )
    out = df.with_columns(
        x=pl.Series(relaxed[:, 0]),
        y=pl.Series(relaxed[:, 1]),
    )
    out.write_parquet(args.out)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
