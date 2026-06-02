#!/usr/bin/env bash
# Run inside the FHS env on <gpu-host>:  $FHS/bin/mapgen-fhs run_image_fusion.sh
# 1) encode thumbnails once, 2) sweep image weights: fuse -> UMAP -> cluster
# (no LLM labels in the sweep; we just want cluster-count/noise to find a weight
# that adds visual signal WITHOUT collapsing the map like the captions did).
set -e
cd ~/mapgen-run

echo ===IMG-ENCODE===
uv run mapgen-embed-image \
  --meta artifacts/embed_meta.parquet \
  --worlds data/worlds_search.parquet \
  --images-dir thumbs_tree/images \
  --out artifacts/image_embeddings.npy

for W in 0.3 0.5 0.8; do
  OUT=artifacts_img_${W}
  mkdir -p "$OUT"
  cp artifacts/embed_meta.parquet "$OUT/embed_meta.parquet"
  echo "=== FUSE+REDUCE+CLUSTER w_image=${W} ==="
  uv run mapgen-fuse \
    --block artifacts/embeddings.npy 1.0 \
    --block artifacts/image_embeddings.npy "$W" \
    --out "$OUT/fused.npy"
  uv run mapgen-reduce --embeddings "$OUT/fused.npy" --meta "$OUT/embed_meta.parquet" \
    --out-dir "$OUT" --methods umap
  uv run mapgen-cluster --embeddings "$OUT/fused.npy" --meta "$OUT/embed_meta.parquet" \
    --out-dir "$OUT" --min-samples 5 --no-label
done
echo ===IMGFUSEDONE===
