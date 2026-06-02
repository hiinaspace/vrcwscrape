"""Diagnose the UMAP-10d-for-clustering collapse (3 clusters / 0 noise).

The fusion sweep collapsed at w_image=0.5 but was healthy at 0.3 and 0.8 -- a
non-monotonic pattern that points at UMAP-10d instability, not a semantic effect.
Test seed-sensitivity, min_dist, and init on the collapsed fused vector; the mean
per-dim variance of the 10d output reveals a degenerate (collapsed) embedding.
"""

from __future__ import annotations

import sys

import numpy as np

from mapgen.common import l2_normalize


def main() -> None:
    import hdbscan
    import umap

    path = sys.argv[1] if len(sys.argv) > 1 else "artifacts_img_0.5/fused.npy"
    emb = l2_normalize(np.load(path))
    print(f"input: {path} shape={emb.shape}")

    def run(seed: int, min_dist: float, init: str) -> None:
        low = umap.UMAP(
            n_components=10,
            n_neighbors=30,
            min_dist=min_dist,
            metric="euclidean",
            random_state=seed,
            init=init,
        ).fit_transform(emb)
        lab = hdbscan.HDBSCAN(
            min_cluster_size=50, min_samples=5, metric="euclidean"
        ).fit_predict(low)
        nc = len({c for c in lab if c != -1})
        nn = int((lab == -1).sum())
        var = float(np.asarray(low).var(axis=0).mean())
        print(
            f"  seed={seed} min_dist={min_dist} init={init:8s}: "
            f"{nc:>3} clusters, {nn:>6,} noise  meanvar10d={var:.4f}"
        )

    for s in (42, 0, 7):
        run(s, 0.0, "spectral")
    run(42, 0.1, "spectral")
    run(42, 0.0, "random")
    run(42, 0.0, "pca")


if __name__ == "__main__":
    main()
