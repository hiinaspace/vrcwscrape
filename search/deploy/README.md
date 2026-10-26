# Publish prep (Cloudflare Pages `worldsearch`)

Nothing here deploys. `deploy.sh` builds/reuses `search/dist`, stamps `about.html`, installs `_headers`, runs `check.py`, and
prints the deploy commands for the owner (or orchestrator) to run from natto after an explicit go.

| file | purpose |
|---|---|
| `deploy.sh [--build] [--no-thumbs] [--test] [--dist DIR]` | build (into `dist.new`, swapped into `dist` only after the checks pass) + finalize + check + print commands |
| `finalize.py DIST PUBLIC_PARQUET` | stamps `about.html` (snapshot = newest `last_scrape_time`, world count) and copies `_headers` into DIST |
| `check.py DIST` | preflight: file count/size limits, image-host refs, non-public world ids, unstamped about, gitleaks, summary |
| `_headers` | Pages headers: CSP (self + data:/blob: images), nosniff, referrer policy, immutable cache for hashed/versioned files, 5 min for html/manifest, octet-stream + no-transform for `.bin` and thumb packs |
| `serve_headers.py DIST [PORT]` | local Pages-like server (applies `_headers`, Range 206) |
| `csp_check.mjs URL` | headless Chromium run against that server: fails on CSP violations, console errors, 4xx, cross-origin requests, unversioned data URLs |

## Thumbnails (not in the first test candidate)
Once `/work/projects/vrcwscrape-data/thumbs/thumbs.parquet` exists (data builder merges `thumbs/parts/`):

    cd /work/projects/vrcwscrape-search/search && deploy/deploy.sh --build      # reuses sim/sem from dist, packs thumbs (<=24 MiB), checks, swaps

then re-run the CSP check against a headers-serving copy (thumbs load as `blob:` images, `img-src` allows them), freeze a copy
(`cp -a dist /work/projects/vrcwscrape-data/deploy-candidate-<ts>`), and use `deploy.sh --dist <that> ` to print commands.
Packs use `?v=<content hash of that pack>` URLs (`manifest.thumbs.hashes`), so they are cached `immutable` and Range-read, and an unchanged pack keeps its URL across builds.

## Notes
- `cf pages deploy` in cf@1.0.0-beta.20 is a stub that refuses; the deploy step is `wrangler pages deploy` (needs `wrangler login` or CLOUDFLARE_API_TOKEN on natto).
- CSP keeps `style-src 'unsafe-inline'`: the app sets inline `style` attributes; strict `style-src 'self'` produces violations (tested).
- `_headers` cache rules are only safe because every `/data` and `/thumbs` fetch carries `?v=`; `csp_check.mjs` asserts that.
- Custom domain needs an explicit proxied CNAME (zone has a `*.hiina.space` wildcard to another host).

## Weekly refresh (k8s)
The weekly rebuild runs unattended as the CronJob `search-refresh` (fleet-infra `infra/my-cluster/vrcw/`, README section "Weekly search refresh");
its code is `search/refresh/` (`refresh.py` orchestrator, `embed_delta.py`, `thumbs_delta.py`, `gate.py`, `seed_state.py`, `Containerfile`).
It does not use `deploy.sh`: it calls `build.sh` with `THUMB_STATE`/`FINALIZE_ARGS`, then `gate.py` (which wraps `check.py`), then `wrangler pages deploy`.
Incremental thumbnails: `build/pack_thumbs.py --state` keeps blob ids and existing packs frozen (new blobs go to new `p-NNN.bin`; repack above 10%).
The browser oracle is not part of the job. Attended: `deploy/serve_headers.py <dist> 4317`, then `cd oracle && uv run python run.py --url http://localhost:4317 --parquet <public-v2.parquet> --dist <dist>`.
