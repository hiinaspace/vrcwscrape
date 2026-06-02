# VRChat Worlds — Latent Map (web)

Interactive map of VRChat worlds laid out by text-embedding similarity, with a
precomputed land polygon, zoom-LOD neighborhood labels, region backgrounds, and a
click-to-inspect sidebar. The full 218k export is the default; `?data=20k` loads the
smaller comparison export.

Stack: React + Vite, deck.gl (`OrthographicView`), DuckDB-WASM (parquet in the
browser). Supersedes the vanilla `web-prototype/`.

## Run

```bash
npm install
npm run dev      # http://localhost:5173
```

## Data

Static assets in `public/` are produced offline by `mapgen-app-export` (see
`../mapgen`), which runs on `<gpu-host>` against the toponymy artifacts and copies the
output here:

```bash
# on <gpu-host>, in ~/mapgen-run
mapgen-fhs -c "uv run python -m mapgen.export_app \
  --coords artifacts/coords_umap_relaxed.parquet \
  --topo-dir artifacts_topo \
  --worlds data/worlds_search.parquet \
  --embeddings artifacts/embeddings.npy \
  --embed-meta artifacts/embed_meta.parquet \
  --out-dir app_export"
# then locally:
rsync -av <gpu-host>:mapgen-run/app_export/ web/public/
```

Assets:
- `app_points.parquet` — per-world: `x,y`, hierarchy ids/names `l0..lN`, soft-assigned
  `region` (continent) + `color`, `name`, `visits`.
- `manifest.json` — lightweight level/asset metadata used before DuckDB starts.
- `land.geojson` — precomputed rasterized/dilated landmass from all world coords.
- `regions_l2.geojson`, `regions_l3.geojson` — background polygons per cluster,
  colored by parent continent.
- `worlds_meta.parquet` — sidebar fields (description, tags, author, dates, sizes…).

## How the LOD labels work

Region + world-title label candidates are projected and greedily decluttered in JS
before deck.gl renders them. Priority brackets (continent ≫ sub-region ≫ world-by-
visits) guarantee coarse labels win when space is tight; as you zoom in, freed screen
space reveals finer labels and finally individual world titles. World-title candidates
are viewport-culled and capped before they reach `TextLayer`.

## Deferred

Faceted search/list (salvage the Mosaic crossfilter from `web-prototype`), a
deep-zoom layout transform (de-overlap/grid), thumbnails, and a date scrubber.
