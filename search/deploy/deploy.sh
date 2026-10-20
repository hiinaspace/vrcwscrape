#!/usr/bin/env bash
# Publish prep for the static world search. Builds (or reuses) search/dist, stamps about.html, installs _headers, runs the
# preflight (check.py), then PRINTS the deploy commands. It never deploys, uploads, or calls a Cloudflare write API.
#
# usage: search/deploy/deploy.sh [--build] [--no-thumbs] [--test] [--dist DIR]
#   --build       rebuild into a temp dir (search/dist.new) with the thumbnail packs, run the checks there, then swap into search/dist
#                 (a failed check leaves search/dist untouched and the new build in search/dist.new for inspection).
#   --no-thumbs   with --build: skip thumbnails even if thumbs.parquet exists (site then shows placeholders)
#   --test        print only the *.pages.dev test-deploy commands (no custom domain)
#   (default)     reuse the existing dist: finalize + check only.
# env: PQ (public v2 parquet), FULL (full parquet for the private-id cross-check), THUMBS (thumbs.parquet), PROJECT, DOMAIN,
#      NATTO_DIR (where the dist is copied on natto; printed commands only)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SEARCH="$(cd "$HERE/.." && pwd)"
DATA="${DATA:-/work/projects/vrcwscrape-data}"
PQ="${PQ:-$DATA/worlds-2026-10-20-public-v2.parquet}"
FULL="${FULL:-$DATA/worlds-2026-10-20.parquet}"
THUMBS="${THUMBS:-$DATA/thumbs/thumbs.parquet}"
PROJECT="${PROJECT:-worldsearch}"
DOMAIN="${DOMAIN:-worldsearch.hiina.space}"
NATTO_DIR="${NATTO_DIR:-\$HOME/scratch/worldsearch-dist}"
DIST="$SEARCH/dist"; BUILD=0; USE_THUMBS=1; TEST=0
while [ $# -gt 0 ]; do
  case "$1" in
    --build) BUILD=1 ;;
    --no-thumbs) USE_THUMBS=0 ;;
    --test) TEST=1 ;;
    --dist) DIST="$(cd "$2" && pwd)"; shift ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac; shift
done
PY=(uv run --project "$SEARCH/build" python)

if [ "$BUILD" = 1 ]; then
  NEW="$SEARCH/dist.new"
  if [ "$USE_THUMBS" = 1 ] && [ ! -f "$THUMBS" ]; then
    echo "no $THUMBS yet (thumbnails still encoding?). Use --no-thumbs to build without." >&2; exit 3
  fi
  [ "$USE_THUMBS" = 1 ] || THUMBS=/nonexistent
  # sim/ + sem/ are reused from the current dist (same world order = same public parquet); drop REUSE_DATA to recompute (~8 min + 33 MB assets).
  REUSE=""; [ -f "$SEARCH/dist/data/manifest.json" ] && REUSE="$SEARCH/dist/data"
  DIST="$NEW" WORK="$SEARCH/work.pub" THUMBS="$THUMBS" REUSE_DATA="$REUSE" PUBLISH_PQ="$PQ" "$SEARCH/build.sh" "$PQ"
  rm -rf "$SEARCH/work.pub"
  DIST="$NEW"
fi

"${PY[@]}" "$HERE/finalize.py" "$DIST" "$PQ"
echo
if ! "${PY[@]}" "$HERE/check.py" "$DIST" --full-parquet "$FULL" --public-parquet "$PQ"; then
  echo "PREFLIGHT FAILED: nothing swapped, nothing to deploy." >&2
  exit 1
fi

if [ "$BUILD" = 1 ]; then  # swap only after the checks passed
  rm -rf "$SEARCH/dist.old"; mv "$SEARCH/dist" "$SEARCH/dist.old"; mv "$SEARCH/dist.new" "$SEARCH/dist"; rm -rf "$SEARCH/dist.old"
  DIST="$SEARCH/dist"; echo "swapped into $DIST"
fi

STAMP="$(date -u +%F)"
echo
echo "================================================================================================="
echo "READY ($(find "$DIST" -type f | wc -l) files in $DIST). Nothing has been deployed. After the owner's explicit go, run (NOT executed here):"
cat <<EOF

# 1. on natto: copy the checked dist (the cf CLI is logged in on natto, not on cc-0)
mkdir -p $NATTO_DIR && rsync -a --delete cc:$DIST/ $NATTO_DIR/

# 2. on natto: cf auth (the token lasts about a day)
npx -y cf@1.0.0-beta.20 auth whoami          # if expired: npx -y cf@1.0.0-beta.20 auth login

# 3. create the Pages project (direct upload, production branch main)
npx -y cf@1.0.0-beta.20 pages create --name $PROJECT --production-branch main

# 4. deploy. 'cf pages deploy' is a stub that refuses ("use npx wrangler pages deploy"), so this step is wrangler.
#    wrangler on natto is NOT logged in (oauth expired): run 'npx -y wrangler@4.151.3 login' once (browser), or export CLOUDFLARE_API_TOKEN.
npx -y wrangler@4.151.3 pages deploy $NATTO_DIR --project-name $PROJECT --branch main --commit-dirty=true --commit-message "snapshot $STAMP"

# 5. smoke test (read-only)
curl -sI https://$PROJECT.pages.dev/ | grep -i 'HTTP\|content-security\|cache-control'
curl -s https://$PROJECT.pages.dev/data/manifest.json | head -c 120; echo
EOF
if [ "$TEST" = 0 ]; then
  cat <<EOF

# 6. LATER: custom domain + proxied CNAME (the zone has a *.hiina.space wildcard to another host, so the explicit record is
#    required; same pattern as the existing nyaastats / orbital records)
npx -y cf@1.0.0-beta.20 pages domains create $PROJECT --name $DOMAIN
npx -y cf@1.0.0-beta.20 dns records create --zone hiina.space --body '{"type":"CNAME","name":"worldsearch","content":"$PROJECT.pages.dev","proxied":true,"ttl":1}'
npx -y cf@1.0.0-beta.20 pages domains get $DOMAIN --project-name $PROJECT    # wait for "active"
EOF
fi
echo "================================================================================================="
