#!/usr/bin/env bash
# Full ~218k-world pipeline (separate artifacts so it can't disturb the live 20k
# viewer). Invoked inside the FHS env:  $FHS/bin/mapgen-fhs run_full.sh
set -euo pipefail
cd ~/mapgen-run
OUT=artifacts_full

echo "=== embed (bge-m3, all worlds) ==="
uv run mapgen-embed --input data/worlds_search.parquet --out-dir "$OUT"

echo "=== reduce (umap only) ==="
uv run mapgen-reduce --embeddings "$OUT/embeddings.npy" --meta "$OUT/embed_meta.parquet" \
  --out-dir "$OUT" --methods umap

echo "=== relax 2D coords (open dense piles for deep zoom) ==="
uv run mapgen-relax --coords "$OUT/coords_umap.parquet" \
  --out "$OUT/coords_umap_relaxed.parquet"

echo "=== toponymy (hierarchy + short labels) ==="
uv run mapgen-toponymy --embeddings "$OUT/embeddings.npy" --meta "$OUT/embed_meta.parquet" \
  --coords "$OUT/coords_umap_relaxed.parquet" --out-dir "$OUT" \
  --name-instructions "Make topic_name a terse map label: 2-3 words, Title Case, at most 24 characters, like 'Cozy Sleep Rooms', 'Horror Escape', 'Avatar Hubs', 'Tropical Beaches'. Output ONLY the short label as the value. Never a sentence or description. Do NOT use 'and', 'for', 'with', or commas."

echo "=== app-export (-> app_export_full) ==="
uv run mapgen-app-export --coords "$OUT/coords_umap_relaxed.parquet" --topo-dir "$OUT" \
  --worlds data/worlds_search.parquet --embeddings "$OUT/embeddings.npy" \
  --embed-meta "$OUT/embed_meta.parquet" --out-dir app_export_full

echo "FULL_DONE"
