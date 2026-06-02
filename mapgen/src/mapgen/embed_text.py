"""Stage B: embed per-world text with bge-m3 (via ollama).

Reads worlds_search.parquet -> writes embeddings.npy (float32, n x dim) and an
aligned embed_meta.parquet (world_id, name, text) in row order.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import polars as pl

from mapgen.common import DEFAULT_EMBED_MODEL, OllamaEmbedder, build_world_text


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, required=True, help="worlds_search.parquet")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--model", default=DEFAULT_EMBED_MODEL)
    ap.add_argument("--ollama-url", default=None)
    ap.add_argument("--batch-size", type=int, default=128)
    ap.add_argument("--limit", type=int, default=None, help="cap rows")
    ap.add_argument(
        "--top-by",
        default=None,
        help="select the top --limit rows by this column desc (e.g. visits) "
        "instead of the first N; gives a representative popular subset",
    )
    ap.add_argument(
        "--captions",
        type=Path,
        default=None,
        help="optional captions.parquet (world_id, caption) to append to the "
        "embedding text (image-caption enrichment)",
    )
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    cols = ["world_id", "name", "description", "tags"]
    if args.top_by and args.top_by not in cols:
        cols.append(args.top_by)
    df = pl.read_parquet(args.input).select(cols)
    if args.top_by:
        df = df.sort(args.top_by, descending=True, nulls_last=True)
    if args.limit:
        df = df.head(args.limit)
    df = df.select("world_id", "name", "description", "tags")

    captions: dict[str, str] = {}
    if args.captions:
        cdf = pl.read_parquet(args.captions)
        captions = dict(
            zip(cdf["world_id"].to_list(), cdf["caption"].to_list(), strict=True)
        )
        print(f"Loaded {len(captions):,} captions from {args.captions}")

    texts = []
    for r in df.iter_rows(named=True):
        text = build_world_text(r["name"], r["description"], r["tags"])
        cap = captions.get(r["world_id"])
        if cap:
            text = f"{text}\nScene: {cap}"
        texts.append(text)
    print(f"Embedding {len(texts):,} worlds with {args.model} ...")

    embedder = OllamaEmbedder(
        model=args.model, base_url=args.ollama_url, batch_size=args.batch_size
    )
    emb = embedder.embed(texts)
    print(f"  embeddings: {emb.shape} dtype={emb.dtype}")

    np.save(args.out_dir / "embeddings.npy", emb)
    meta = df.select("world_id", "name").with_columns(pl.Series("text", texts))
    meta.write_parquet(args.out_dir / "embed_meta.parquet")
    print(f"  wrote {args.out_dir / 'embeddings.npy'} and embed_meta.parquet")


if __name__ == "__main__":
    main()
