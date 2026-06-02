"""Does raising the image weight rescue thin-text worlds (their whole rationale)?

Sweep image weight and, with leaf selection, report cluster count + noise rate
split by thin-text vs rich-text. If the image block carries cluster-forming
signal for weak-text worlds, thin-noise should fall as weight rises (ideally
faster than rich-noise). If thin-noise stays flat, CLIP-on-thumbnails isn't
adding usable structure and image fusion isn't worth it.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from mapgen.common import l2_normalize


def main() -> None:
    import hdbscan
    import umap

    thin_set = set(pl.read_parquet("artifacts/captions.parquet")["world_id"].to_list())
    ids = pl.read_parquet("artifacts/embed_meta.parquet")["world_id"].to_list()
    thin = np.array([w in thin_set for w in ids])

    def l2(x: np.ndarray) -> np.ndarray:
        n = np.linalg.norm(x, axis=1, keepdims=True)
        n[n < 1e-12] = 1.0
        return x / n

    t = l2(np.load("artifacts/embeddings.npy").astype("float32"))
    m = l2(np.load("artifacts/image_embeddings.npy").astype("float32"))

    for w in (0.0, 0.3, 0.5, 0.8, 1.5, 2.0):
        x = t if w == 0 else np.concatenate([t, w * m], axis=1)
        low = umap.UMAP(
            n_components=10,
            n_neighbors=30,
            min_dist=0.0,
            metric="euclidean",
            random_state=42,
        ).fit_transform(l2_normalize(x))
        lab = hdbscan.HDBSCAN(
            min_cluster_size=50,
            min_samples=5,
            metric="euclidean",
            cluster_selection_method="leaf",
        ).fit_predict(low)
        noise = lab == -1
        nc = len(set(lab[~noise].tolist()))
        tn = 100 * (noise & thin).sum() / thin.sum()
        rn = 100 * (noise & ~thin).sum() / (~thin).sum()
        print(
            f"  w_img={w:>4}: {nc:>3} clusters | "
            f"thin-text noise {tn:4.0f}% | rich-text noise {rn:4.0f}%"
        )


if __name__ == "__main__":
    main()
