"""One-off: build a relative-path list of thumbnails for the embed subset.

Joins embed_meta.parquet (the subset, in row order) with worlds_search.parquet
to get thumb_sha256, and writes `images/<2>/<2>/<sha>.png` lines for rsync.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--meta", type=Path, default=Path("artifacts/embed_meta.parquet"))
    ap.add_argument("--worlds", type=Path, default=Path("data/worlds_search.parquet"))
    ap.add_argument("--out", type=Path, default=Path("thumb_paths_20k.txt"))
    args = ap.parse_args()

    meta = pl.read_parquet(args.meta).select("world_id")
    ws = pl.read_parquet(args.worlds).select("world_id", "thumb_sha256")
    j = meta.join(ws, on="world_id", how="left")
    have = j.filter(pl.col("thumb_sha256").is_not_null())
    print(
        f"subset rows: {j.height} with thumb: {have.height} "
        f"missing: {j.height - have.height}"
    )
    shas = have["thumb_sha256"].unique().to_list()
    with args.out.open("w") as f:
        for s in shas:
            f.write(f"images/{s[:2]}/{s[2:4]}/{s}.png\n")
    print(f"unique shas: {len(shas)} -> {args.out}")


if __name__ == "__main__":
    main()
