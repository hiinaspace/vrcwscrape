# mapgen

Offline pipeline that turns the VRChat worlds parquet into a labeled 2D "latent
map": **embeddings → dimensionality reduction → clustering → labeled static map**.
Phase 1 ends at a GO/NO-GO gate (eyeball the map + NN report) before any frontend
gets built. See `/home/s/.claude/plans/modular-shimmying-papert.md`.

Pure-CPU Python venv (numpy/umap/pacmap/hdbscan/datamapplot); all model inference
goes through **ollama** (bge-m3 embeddings + gemma4 labels) so there's no
torch/CUDA to set up. The GPU box `<gpu-host>` runs ollama; run the whole pipeline there.

## Data prep (Stage A, in the root project)

```bash
# from repo root, with a local `dolt sql-server` running on ~/Downloads/vrcwscrape
DATABASE_URL='mysql://root@127.0.0.1:3306/vrcwscrape' \
  uv run --with duckdb --with pymysql --with python-dotenv \
  python3 scripts/etl.py --output-dir mapgen/data --worlds-limit 20000 --packages-limit 60000
```

## Pipeline (on <gpu-host>, inside the FHS shell)

NixOS needs the FHS env (see `fhs.nix`). Wrap each command:

```bash
FHS=$(nix-build --no-out-link fhs.nix)   # build once
$FHS/bin/mapgen-fhs -c 'uv sync && uv run mapgen-embed ...'
```

Stages (top-20k-by-visits subset shown):

```bash
# B: text embeddings (bge-m3 via ollama); --top-by picks a representative popular subset
uv run mapgen-embed --input data/worlds_search.parquet --out-dir artifacts --top-by visits --limit 20000
# C: dimensionality reduction (UMAP / PaCMAP / LocalMAP)
uv run mapgen-reduce --embeddings artifacts/embeddings.npy --meta artifacts/embed_meta.parquet --out-dir artifacts
# C2: local coordinate relaxation for deep-zoom legibility; preserves global shape,
# but opens up ultra-dense piles so Voronoi cells and labels have room.
uv run mapgen-relax --coords artifacts/coords_umap.parquet --out artifacts/coords_umap_relaxed.parquet
# D: HDBSCAN clusters + gemma4 labels + per-cluster exemplar report
uv run mapgen-cluster --embeddings artifacts/embeddings.npy --meta artifacts/embed_meta.parquet --out-dir artifacts --min-samples 5
# E: labeled region map (PNG + GeoJSON; raster land/region polygons for the web app)
uv run mapgen-regions --embeddings artifacts/embeddings.npy --meta artifacts/embed_meta.parquet \
  --clusters artifacts/clusters.parquet --labels artifacts/cluster_labels.json --coords-dir artifacts --out-dir artifacts
uv run mapgen-render --embeddings artifacts/embeddings.npy --meta artifacts/embed_meta.parquet \
  --clusters artifacts/clusters.parquet --labels artifacts/cluster_labels.json --coords-dir artifacts --out-dir artifacts
```

Copy `artifacts/regions_*.png`, `cluster_exemplars.md`, `map_*.html`, `nn_report.md` back and eyeball.

For the full 218k app export and layout comparison, use the scripts in
`mapgen/scripts/` inside the FHS shell:

```bash
$FHS/bin/mapgen-fhs run_full.sh              # UMAP baseline
$FHS/bin/mapgen-fhs run_layout_variants.sh   # refresh UMAP, build PaCMAP/LocalMAP
```

## Image-caption enrichment (optional)

Caption thin-description worlds' thumbnails with gemma4 (vision) and fold the
caption into the embedding text. Thumbnails: `rsync` the needed
`images/<2>/<2>/<sha>.png` from <image-host> to `thumbs_tree/` first.

```bash
uv run mapgen-caption --input data/worlds_search.parquet --images-dir thumbs_tree/images \
  --out artifacts/captions.parquet --restrict-to artifacts/embed_meta.parquet --max-desc-len 40
uv run mapgen-embed ... --captions artifacts/captions.parquet   # appends "Scene: <caption>"
```
