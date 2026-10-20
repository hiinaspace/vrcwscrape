#!/usr/bin/env bash
# env: DIST, WORK override dirs; STATIC_DIR=<emb/static> enables the semantic assets.
# usage: search/build.sh <worlds.parquet> [embeddings.npy ids.parquet]  ->  search/dist/ (static site)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PQ="${1:?parquet path}"; EMB="${2:-}"; IDS="${3:-}"
DIST="${DIST:-$HERE/dist}"; WORK="${WORK:-$HERE/work}"
T0=$(date +%s)
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-6}"
rm -rf "$DIST"; mkdir -p "$DIST" "$WORK"
( cd "$HERE/web" && { [ -d node_modules ] || npm install --no-audit --no-fund >/dev/null; } && npx vite build --outDir "$DIST" --emptyOutDir ) &
WEB=$!
uv run --project "$HERE/build" python "$HERE/build/build_index.py" "$PQ" "$WORK/data" --work "$WORK"
if [ -n "$EMB" ]; then
  uv run --project "$HERE/build" python "$HERE/build/similar.py" "$EMB" "$IDS" "$WORK/world_order.parquet" "$WORK/data"
fi
if [ -n "${STATIC_DIR:-}" ]; then  # opt-in browser semantic search assets (needs IDS = full-row-order ids parquet)
  uv run --project "$HERE/build" python "$HERE/build/semantic.py" "$STATIC_DIR" "$IDS" "$WORK/world_order.parquet" "$WORK/data"
fi
wait $WEB
mv "$WORK/data" "$DIST/data"
T1=$(date +%s)
echo "build done in $((T1-T0))s -> $DIST"
du -sh "$DIST"; find "$DIST" -type f | wc -l
find "$DIST" -type f -printf '%s %p\n' | sort -nr | head -3
