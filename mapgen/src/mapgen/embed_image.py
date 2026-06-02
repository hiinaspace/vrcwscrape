"""Stage B-image: embed world thumbnails with open-CLIP (a separate modality).

Produces image_embeddings.npy aligned ROW-FOR-ROW with embed_meta.parquet, so it
can be fused with the text embeddings as an independent, separately-weighted block
(see fuse.py) rather than mixed into one text string -- which is what collapsed the
map when we tried folding VLM captions into the bge-m3 text.

Every row gets a unit vector. Missing/corrupt thumbnails are mean-filled (the mean
of the present image vectors) so a world without a usable thumbnail sits at the
"average" image rather than forming a spurious "has-no-thumbnail" cluster.

The heavy torch/open-clip stack is the `image` optional-dependency extra:
    uv sync --extra image
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import polars as pl
from tqdm import tqdm


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--meta",
        type=Path,
        required=True,
        help="embed_meta.parquet -- defines the row order to align to",
    )
    ap.add_argument(
        "--worlds",
        type=Path,
        required=True,
        help="worlds_search.parquet (for thumb_sha256)",
    )
    ap.add_argument(
        "--images-dir",
        type=Path,
        required=True,
        help="root containing <sha[:2]>/<sha[2:4]>/<sha>.png thumbnails",
    )
    ap.add_argument("--out", type=Path, required=True, help="image_embeddings.npy")
    ap.add_argument("--model", default="ViT-B-32")
    ap.add_argument("--pretrained", default="laion2b_s34b_b79k")
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--device", default=None, help="cpu|cuda (default: auto)")
    args = ap.parse_args()

    import open_clip
    import torch
    from PIL import Image

    device = args.device or ("cuda" if torch.cuda.is_available() else "cpu")
    print(f"open-CLIP {args.model}/{args.pretrained} on {device}")
    model, _, preprocess = open_clip.create_model_and_transforms(
        args.model, pretrained=args.pretrained, device=device
    )
    model.eval()

    meta = pl.read_parquet(args.meta).select("world_id")
    ws = pl.read_parquet(args.worlds).select("world_id", "thumb_sha256")
    df = meta.join(ws, on="world_id", how="left")  # left join preserves meta order
    shas = df["thumb_sha256"].to_list()
    n = len(shas)

    paths: list[Path | None] = []
    for s in shas:
        if not s:
            paths.append(None)
            continue
        p = args.images_dir / s[:2] / s[2:4] / f"{s}.png"
        paths.append(p if p.exists() else None)

    emb: np.ndarray | None = None
    present = np.zeros(n, dtype=bool)
    batch_idx: list[int] = []
    batch_img: list[object] = []

    def flush() -> None:
        nonlocal emb, batch_idx, batch_img
        if not batch_idx:
            return
        x = torch.stack(batch_img).to(device)  # type: ignore[arg-type]
        with torch.no_grad():
            f = model.encode_image(x)
            f = f / f.norm(dim=-1, keepdim=True)
        feats = f.cpu().numpy().astype("float32")
        if emb is None:
            emb = np.zeros((n, feats.shape[1]), dtype="float32")
        emb[batch_idx] = feats
        for i in batch_idx:
            present[i] = True
        batch_idx = []
        batch_img = []

    missing = 0
    for i, p in enumerate(tqdm(paths, desc="img-embed")):
        if p is None:
            missing += 1
            continue
        try:
            img = Image.open(p).convert("RGB")
        except Exception as err:  # noqa: BLE001 - skip a corrupt image, keep going
            print(f"  !! {p.name}: {str(err)[:60]}")
            missing += 1
            continue
        batch_img.append(preprocess(img))
        batch_idx.append(i)
        if len(batch_idx) >= args.batch_size:
            flush()
    flush()

    if emb is None:
        raise SystemExit("no images could be embedded")

    n_missing = int((~present).sum())
    if n_missing and present.any():
        mean_vec = emb[present].mean(axis=0)
        nrm = float(np.linalg.norm(mean_vec))
        if nrm > 0:
            mean_vec = mean_vec / nrm
        emb[~present] = mean_vec
    print(
        f"  embedded {int(present.sum()):,}/{n:,} "
        f"({missing:,} missing/corrupt -> mean-filled)"
    )

    np.save(args.out, emb)
    print(f"  wrote {args.out} shape={emb.shape} dtype={emb.dtype}")


if __name__ == "__main__":
    main()
