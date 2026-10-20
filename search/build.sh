#!/usr/bin/env bash
# env: DIST, WORK override dirs; STATIC_DIR=<emb/static> enables the semantic assets.
#      THUMBS=<thumbs.parquet> (default /work/projects/vrcwscrape-data/thumbs/thumbs.parquet; skipped when absent) packs self-hosted AVIFs
#      into $DIST/thumbs (<=24 MiB packs) + data/thumbs_index.bin; THUMBS_BASE=<url> if the packs are hosted elsewhere (default ../thumbs/).
#      PUBLISH_PQ=<public parquet>: also stamp about.html + install deploy/_headers (deploy/finalize.py); deploy/deploy.sh sets it.
#      REUSE_DATA=<old dist/data> copies sim/ and sem/ from a previous build with the identical world order (skips the 8 min neighbour job).
# usage: search/build.sh <worlds.parquet> [embeddings.npy ids.parquet]  ->  search/dist/ (static site)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PQ="${1:?parquet path}"; EMB="${2:-}"; IDS="${3:-}"
DIST="${DIST:-$HERE/dist}"; WORK="${WORK:-$HERE/work}"
T0=$(date +%s)
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-6}"
THUMBS="${THUMBS:-/work/projects/vrcwscrape-data/thumbs/thumbs.parquet}"
rm -rf "$DIST" "$WORK/data" "$WORK/thumbs"; mkdir -p "$DIST" "$WORK"
( cd "$HERE/web" && { [ -d node_modules ] || npm install --no-audit --no-fund >/dev/null; } && npx vite build --outDir "$DIST" --emptyOutDir ) &
WEB=$!
uv run --project "$HERE/build" python "$HERE/build/build_index.py" "$PQ" "$WORK/data" --work "$WORK"
if [ -n "$EMB" ]; then
  uv run --project "$HERE/build" python "$HERE/build/similar.py" "$EMB" "$IDS" "$WORK/world_order.parquet" "$WORK/data"
fi
if [ -n "${STATIC_DIR:-}" ]; then  # opt-in browser semantic search assets (needs IDS = full-row-order ids parquet)
  uv run --project "$HERE/build" python "$HERE/build/semantic.py" "$STATIC_DIR" "$IDS" "$WORK/world_order.parquet" "$WORK/data"
fi
if [ -n "${REUSE_DATA:-}" ]; then  # same world order only (checked by the caller): neighbours + semantic assets
  [ -d "$WORK/data/sim" ] || cp -r "$REUSE_DATA/sim" "$WORK/data/sim"
  [ -d "$WORK/data/sem" ] || { [ ! -d "$REUSE_DATA/sem" ] || cp -r "$REUSE_DATA/sem" "$WORK/data/sem"; }
fi
if [ -f "$THUMBS" ]; then
  uv run --project "$HERE/build" python "$HERE/build/pack_thumbs.py" "$THUMBS" "$WORK/world_order.parquet" "$WORK/thumbs" \
    --manifest "$WORK/data/manifest.json" --index-dir "$WORK/data" --base "${THUMBS_BASE:-../thumbs/}"
fi
wait $WEB
mv "$WORK/data" "$DIST/data"
[ ! -d "$WORK/thumbs" ] || mv "$WORK/thumbs" "$DIST/thumbs"
[ -z "${PUBLISH_PQ:-}" ] || uv run --project "$HERE/build" python "$HERE/deploy/finalize.py" "$DIST" "$PUBLISH_PQ"
T1=$(date +%s)
echo "build done in $((T1-T0))s -> $DIST"
du -sh "$DIST"; find "$DIST" -type f | wc -l
find "$DIST" -type f -printf '%s %p\n' | sort -nr | head -3  || true
