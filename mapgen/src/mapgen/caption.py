"""Image-caption enrichment: caption world thumbnails with a vision LLM.

Many worlds have a thin/empty description but a screenshot that conveys the
vibe. We caption those thumbnails with gemma4 (vision) via ollama and emit
captions.parquet (world_id, caption); embed_text.py can then append the caption
to the embedding text (--captions) so weak-text worlds cluster sensibly.

Thumbnails are expected at <images-dir>/<thumb_sha256>.png (sync them from
<image-host> first).
"""

from __future__ import annotations

import argparse
import base64
from pathlib import Path

import httpx
import polars as pl
from tqdm import tqdm

from mapgen.common import DEFAULT_OLLAMA_URL, ollama_generate

CAPTION_PROMPT = (
    "This is a thumbnail screenshot of a VRChat world. In one short phrase "
    "(<=8 words), describe its setting and vibe (e.g. 'cozy japanese cafe at "
    "night', 'neon cyberpunk dance club'). Reply with only the phrase."
)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, required=True, help="worlds_search.parquet")
    ap.add_argument(
        "--images-dir",
        type=Path,
        required=True,
        help="root containing <sha[:2]>/<sha[2:4]>/<sha>.png thumbnails",
    )
    ap.add_argument("--out", type=Path, required=True, help="captions.parquet")
    ap.add_argument("--model", default="gemma4:e4b")
    ap.add_argument("--ollama-url", default=None)
    ap.add_argument(
        "--restrict-to",
        type=Path,
        default=None,
        help="optional parquet with a world_id column; only caption those",
    )
    ap.add_argument(
        "--max-desc-len",
        type=int,
        default=40,
        help="only caption worlds whose description is shorter than this "
        "(0 = caption all with a thumbnail)",
    )
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    df = pl.read_parquet(args.input).select("world_id", "description", "thumb_sha256")
    df = df.filter(pl.col("thumb_sha256").is_not_null())
    if args.restrict_to:
        keep = pl.read_parquet(args.restrict_to).select("world_id").unique()
        df = df.join(keep, on="world_id", how="inner")
    if args.max_desc_len:
        dlen = pl.col("description").fill_null("").str.len_chars()
        df = df.filter(dlen < args.max_desc_len)
    if args.limit:
        df = df.head(args.limit)
    print(f"Captioning {df.height:,} worlds (model={args.model}) ...")

    base_url = args.ollama_url or DEFAULT_OLLAMA_URL
    rows: list[dict] = []
    missing = 0
    with httpx.Client(timeout=180.0) as client:
        for r in tqdm(df.iter_rows(named=True), total=df.height, desc="caption"):
            sha = r["thumb_sha256"]
            img = args.images_dir / sha[:2] / sha[2:4] / f"{sha}.png"
            if not img.exists():
                missing += 1
                continue
            b64 = base64.b64encode(img.read_bytes()).decode()
            try:
                cap = ollama_generate(
                    CAPTION_PROMPT,
                    model=args.model,
                    base_url=base_url,
                    images=[b64],
                    client=client,
                ).strip()
            except Exception as err:  # noqa: BLE001 - skip a bad image, keep going
                print(f"  !! {sha[:12]}: {str(err)[:60]}")
                continue
            rows.append({"world_id": r["world_id"], "caption": cap})

    out = pl.DataFrame(rows, schema={"world_id": pl.Utf8, "caption": pl.Utf8})
    out.write_parquet(args.out)
    print(f"  wrote {out.height:,} captions to {args.out} ({missing:,} thumbs missing)")


if __name__ == "__main__":
    main()
