#!/usr/bin/env bash
# Fair side-by-side: text-only vs image-fused (w=0.3), both with leaf selection,
# labeled + region maps. Run: $FHS/bin/mapgen-fhs run_leaf_compare.sh
set -e
cd ~/mapgen-run

echo ===TEXT-LEAF-CLUSTER===
uv run mapgen-cluster --embeddings artifacts/embeddings.npy --meta artifacts/embed_meta.parquet \
  --out-dir artifacts_text_leaf --selection leaf --min-samples 5
echo ===TEXT-LEAF-REGIONS===
uv run mapgen-regions --embeddings artifacts/embeddings.npy --meta artifacts/embed_meta.parquet \
  --clusters artifacts_text_leaf/clusters.parquet --labels artifacts_text_leaf/cluster_labels.json \
  --coords-dir artifacts --out-dir artifacts_text_leaf

echo ===IMG0.3-LEAF-CLUSTER===
uv run mapgen-cluster --embeddings artifacts_img_0.3/fused.npy --meta artifacts_img_0.3/embed_meta.parquet \
  --out-dir artifacts_img0.3_leaf --selection leaf --min-samples 5
echo ===IMG0.3-LEAF-REGIONS===
uv run mapgen-regions --embeddings artifacts_img_0.3/fused.npy --meta artifacts_img_0.3/embed_meta.parquet \
  --clusters artifacts_img0.3_leaf/clusters.parquet --labels artifacts_img0.3_leaf/cluster_labels.json \
  --coords-dir artifacts_img_0.3 --out-dir artifacts_img0.3_leaf
echo ===LEAFCOMPAREDONE===
