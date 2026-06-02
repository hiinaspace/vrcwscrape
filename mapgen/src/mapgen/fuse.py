"""Fuse modality blocks (text, image, ...) into one feature matrix for DR/cluster.

Each block is L2-normalized PER ROW, then scaled by its scalar weight, then the
blocks are concatenated. Downstream DR/clustering uses euclidean distance, so

    d(i, j)^2  =  sum_block  2 * w_block^2 * (1 - cos_block(i, j))

i.e. every --block weight is the *only* knob on that modality's influence, and a
big high-dimensional block can't silently dominate a small one. Because each block
is unit-norm, every fused row has the same norm (sqrt(sum w^2)), so any later
whole-vector L2-normalize is a harmless global scale that preserves the weighting.

    mapgen-fuse --block artifacts/embeddings.npy 1.0 \\
                --block artifacts/image_embeddings.npy 0.5 \\
                --out artifacts/fused.npy
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np


def _l2_rows(x: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(x, axis=1, keepdims=True)
    n[n < 1e-12] = 1.0
    return x / n


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--block",
        action="append",
        nargs=2,
        metavar=("NPY", "WEIGHT"),
        required=True,
        help="repeatable: a .npy block and its scalar weight",
    )
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    parts: list[np.ndarray] = []
    n: int | None = None
    for path, w in args.block:
        x = np.load(path).astype("float32")
        if n is None:
            n = x.shape[0]
        elif x.shape[0] != n:
            raise SystemExit(f"row mismatch: {path} has {x.shape[0]} rows, want {n}")
        parts.append(_l2_rows(x) * float(w))
        print(f"  block {path}: shape={x.shape} weight={w}")

    fused = np.concatenate(parts, axis=1).astype("float32")
    np.save(args.out, fused)
    print(f"wrote {args.out} shape={fused.shape}")


if __name__ == "__main__":
    main()
