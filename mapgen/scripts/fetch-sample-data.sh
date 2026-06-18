#!/usr/bin/env bash
# Fetch the sample mapgen data bundle so the offline pipeline / tests /
# regional (R1) probe can run without a local Dolt clone or the GPU box.
#
# These are convenience copies of public VRChat world metadata, hosted as
# disposable assets (not the canonical source; final hosting will differ).
# Source of truth is the Dolt DB: https://www.dolthub.com/repositories/hiinaspace/vrcwscrape
#
# Files land at their repo-relative paths. Run from anywhere; paths are
# resolved relative to the repo root.
#
#   bash mapgen/scripts/fetch-sample-data.sh
#
# Override the mirror with BASE_URL=... if you rehost it.
set -euo pipefail

BASE_URL="${BASE_URL:-https://file.hiina.space/vrcwscrape-mapgen}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Repo-relative paths to fetch (mirror the layout on the mirror).
FILES=(
  # Raw ETL output: input to the embed stage (Stage A in mapgen/README.md).
  "mapgen/data/worlds_search.parquet"
  # Derived artifacts: skip the GPU embed/reduce/cluster stages and go
  # straight to render / regions / dr-sweep on real data.
  "mapgen/artifacts/embeddings.npy"
  "mapgen/artifacts/embed_meta.parquet"
  "mapgen/artifacts/clusters.parquet"
  "mapgen/artifacts/cluster_labels.json"
  "mapgen/artifacts/coords_umap.parquet"
  "mapgen/artifacts/coords_pacmap.parquet"
  "mapgen/artifacts/coords_localmap.parquet"
  # Regional (R1) probe input: run_r1_island_inputs.py reads these two.
  "web/public/full-nolabs-localmap-island-toponymy-city-mesh/app_points.parquet"
  "web/public/full-nolabs-localmap-island-toponymy-city-mesh/land.geojson"
)

echo "Fetching mapgen sample data into $REPO_ROOT"
for rel in "${FILES[@]}"; do
  dest="$REPO_ROOT/$rel"
  echo "  -> $rel"
  curl -fL --create-dirs --progress-bar -o "$dest" "$BASE_URL/$rel"
done

# Optional integrity check against the published manifest.
if command -v sha256sum >/dev/null 2>&1; then
  echo "Verifying checksums..."
  tmp_manifest="$(mktemp)"
  trap 'rm -f "$tmp_manifest"' EXIT
  if curl -fsSL -o "$tmp_manifest" "$BASE_URL/MANIFEST.sha256"; then
    (cd "$REPO_ROOT" && sha256sum -c "$tmp_manifest")
  else
    echo "  (no MANIFEST.sha256 on mirror; skipping verification)"
  fi
fi
echo "Done. See mapgen/README.md ('Sample data' section) for what each file unlocks."
