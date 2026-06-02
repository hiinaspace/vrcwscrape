"""Stage C: reduce embeddings to 2D with several DR methods for comparison.

Writes coords_<method>.parquet (world_id, x, y) for each requested method.
Embeddings are L2-normalized first so euclidean-based methods approximate cosine.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np
import polars as pl

from mapgen.common import l2_normalize


def _run_umap(
    x: np.ndarray, n_neighbors: int, min_dist: float, seed: int
) -> np.ndarray:
    import umap

    reducer = umap.UMAP(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric="euclidean",
        random_state=seed,
    )
    return reducer.fit_transform(x)


def _run_pacmap(
    x: np.ndarray, n_neighbors: int, min_dist: float, seed: int
) -> np.ndarray:
    import pacmap

    reducer = pacmap.PaCMAP(n_components=2, n_neighbors=n_neighbors, random_state=seed)
    return reducer.fit_transform(x, init="pca")


def _run_localmap(
    x: np.ndarray, n_neighbors: int, min_dist: float, seed: int
) -> np.ndarray:
    import pacmap

    if not hasattr(pacmap, "LocalMAP"):
        raise RuntimeError("installed pacmap has no LocalMAP; upgrade pacmap")
    reducer = pacmap.LocalMAP(
        n_components=2, n_neighbors=n_neighbors, random_state=seed
    )
    return reducer.fit_transform(x, init="pca")


METHODS = {"umap": _run_umap, "pacmap": _run_pacmap, "localmap": _run_localmap}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--embeddings", type=Path, required=True, help="embeddings.npy")
    ap.add_argument("--meta", type=Path, required=True, help="embed_meta.parquet")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--methods", default="umap,pacmap,localmap")
    ap.add_argument("--n-neighbors", type=int, default=15)
    ap.add_argument("--min-dist", type=float, default=0.1)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    emb = l2_normalize(np.load(args.embeddings))
    ids = pl.read_parquet(args.meta).select("world_id")
    print(f"Loaded {emb.shape[0]:,} x {emb.shape[1]} embeddings")

    for method in (m.strip() for m in args.methods.split(",") if m.strip()):
        if method not in METHODS:
            print(f"  !! unknown method {method!r}, skipping")
            continue
        t0 = time.time()
        try:
            coords = METHODS[method](emb, args.n_neighbors, args.min_dist, args.seed)
        except Exception as err:  # noqa: BLE001 - report and keep going
            print(f"  !! {method} failed: {err}")
            continue
        out = ids.with_columns(
            x=pl.Series(coords[:, 0].astype("float32")),
            y=pl.Series(coords[:, 1].astype("float32")),
        )
        path = args.out_dir / f"coords_{method}.parquet"
        out.write_parquet(path)
        print(f"  {method}: {coords.shape} in {time.time() - t0:.1f}s -> {path.name}")


if __name__ == "__main__":
    main()
