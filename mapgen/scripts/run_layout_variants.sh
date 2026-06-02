#!/usr/bin/env bash
# Full-data layout variant sweep. Invoked inside the FHS env on sayu:
#   $FHS/bin/mapgen-fhs run_layout_variants.sh
#
# Reuses the existing full text embeddings, regenerates the relaxed UMAP app export
# with the current fast raster land/region polygons, and builds separate PaCMAP and
# LocalMAP toponymy/app exports for visual comparison.
set -euo pipefail
cd ~/mapgen-run

OUT=${OUT:-artifacts_full}
WORLD_PARQUET=${WORLD_PARQUET:-data/worlds_search.parquet}
VARIANT_METHODS=${VARIANT_METHODS:-pacmap,localmap}
EXPORT_UMAP=${EXPORT_UMAP:-app_export_full}
EXPORT_PREFIX=${EXPORT_PREFIX:-app_export_full}
FORCE_TOPONYMY=${FORCE_TOPONYMY:-0}

NAME_INSTRUCTIONS="Make topic_name a terse map label: 2-3 words, Title Case, at most 24 characters, like 'Cozy Sleep Rooms', 'Horror Escape', 'Avatar Hubs', 'Tropical Beaches'. Output ONLY the short label as the value. Never a sentence or description. Do NOT use 'and', 'for', 'with', or commas."

if [[ ! -f "$OUT/embeddings.npy" || ! -f "$OUT/embed_meta.parquet" ]]; then
  echo "missing $OUT/embeddings.npy or $OUT/embed_meta.parquet; run mapgen-embed first" >&2
  exit 1
fi

echo "=== reduce (${VARIANT_METHODS}) ==="
uv run mapgen-reduce --embeddings "$OUT/embeddings.npy" --meta "$OUT/embed_meta.parquet" \
  --out-dir "$OUT" --methods "$VARIANT_METHODS"

echo "=== umap app-export refresh (relaxed coords + raster polygons) ==="
if [[ ! -f "$OUT/coords_umap_relaxed.parquet" ]]; then
  uv run mapgen-relax --coords "$OUT/coords_umap.parquet" \
    --out "$OUT/coords_umap_relaxed.parquet"
fi
uv run mapgen-app-export --coords "$OUT/coords_umap_relaxed.parquet" --topo-dir "$OUT" \
  --worlds "$WORLD_PARQUET" --embeddings "$OUT/embeddings.npy" \
  --embed-meta "$OUT/embed_meta.parquet" --out-dir "$EXPORT_UMAP"

IFS=',' read -r -a methods <<< "$VARIANT_METHODS"
for method in "${methods[@]}"; do
  method="${method// /}"
  [[ -n "$method" ]] || continue

  coords="$OUT/coords_${method}.parquet"
  relaxed="$OUT/coords_${method}_relaxed.parquet"
  topo="$OUT/toponymy_${method}"
  export_dir="${EXPORT_PREFIX}_${method}"

  if [[ ! -f "$coords" ]]; then
    echo "missing $coords after reduction" >&2
    exit 1
  fi

  echo "=== relax ${method} coords ==="
  uv run mapgen-relax --coords "$coords" --out "$relaxed"

  if [[ "$FORCE_TOPONYMY" == "1" || ! -f "$topo/layer_0.parquet" ]]; then
    echo "=== toponymy ${method} (hierarchy + short labels) ==="
    mkdir -p "$topo"
    uv run mapgen-toponymy --embeddings "$OUT/embeddings.npy" --meta "$OUT/embed_meta.parquet" \
      --coords "$relaxed" --out-dir "$topo" \
      --name-instructions "$NAME_INSTRUCTIONS"
  else
    echo "=== toponymy ${method}: existing $topo, skipping ==="
  fi

  echo "=== app-export ${method} (-> ${export_dir}) ==="
  uv run mapgen-app-export --coords "$relaxed" --topo-dir "$topo" \
    --worlds "$WORLD_PARQUET" --embeddings "$OUT/embeddings.npy" \
    --embed-meta "$OUT/embed_meta.parquet" --out-dir "$export_dir"
done

echo "LAYOUT_VARIANTS_DONE"
