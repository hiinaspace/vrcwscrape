#!/usr/bin/env bash
# Short-label toponymy re-run. Invoked inside the FHS env:
#   $FHS/bin/mapgen-fhs run_topo_v2.sh
set -euo pipefail
cd ~/mapgen-run
uv run mapgen-toponymy \
  --embeddings artifacts/embeddings.npy \
  --meta artifacts/embed_meta.parquet \
  --coords artifacts/coords_umap.parquet \
  --out-dir artifacts_topo_v2 \
  --name-instructions "Make topic_name a terse map label: 2-3 words, Title Case, at most 24 characters, like 'Cozy Sleep Rooms', 'Horror Escape', 'Avatar Hubs', 'Tropical Beaches'. Output ONLY the short label as the value. Never a sentence or description. Do NOT use 'and', 'for', 'with', or commas."
echo "TOPO_V2_DONE"
