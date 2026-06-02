"""Test HDBSCAN cluster_selection_method (eom vs leaf) across fusion weights.

The w=0.5 collapse (3 clusters / 0 noise) survived every UMAP seed/init/min_dist
and the 10d variance was healthy (~0.6), so it's not a UMAP collapse -- it's
HDBSCAN's "excess of mass" selection picking one giant parent. "leaf" selection
takes the fine leaf clusters instead. Confirm leaf is stable across weights (and
gives the many-small-clusters we want for the hierarchy).
"""

from __future__ import annotations

import numpy as np

from mapgen.common import l2_normalize

INPUTS = {
    "text-only": "artifacts/embeddings.npy",
    "img0.3": "artifacts_img_0.3/fused.npy",
    "img0.5": "artifacts_img_0.5/fused.npy",
    "img0.8": "artifacts_img_0.8/fused.npy",
}


def main() -> None:
    import hdbscan
    import umap

    for name, path in INPUTS.items():
        emb = l2_normalize(np.load(path))
        low = umap.UMAP(
            n_components=10,
            n_neighbors=30,
            min_dist=0.0,
            metric="euclidean",
            random_state=42,
        ).fit_transform(emb)
        for method in ("eom", "leaf"):
            lab = hdbscan.HDBSCAN(
                min_cluster_size=50,
                min_samples=5,
                metric="euclidean",
                cluster_selection_method=method,
            ).fit_predict(low)
            nc = len({c for c in lab if c != -1})
            nn = int((lab == -1).sum())
            print(f"  {name:10s} {method}: {nc:>4} clusters, {nn:>6,} noise")


if __name__ == "__main__":
    main()
