# Faceted VRChat Worlds Search Prototype

## Goals
- Build a working, client-side Mosaic UI that loads `worlds_search.parquet` in the browser.
- Enable faceted filtering and brushing across key dimensions.
- Add a basic text search input and verify it affects the faceted views.
- Prototype two FTS paths: DuckDB FTS (WASM) and FlexSearch.

## Constraints
- Browser-only (no server).
- Use DuckDB-WASM for query processing.
- Initial dataset: `worlds_search.parquet` (expected ~210k rows).

## Target Columns (expected)
- IDs: `world_id`
- Text: `name`, `description`, `author_name`
- Arrays/tags: `tags`
- Metrics: `visits`, `favorites`, `popularity`, `heat`, `capacity`
- Dates: `created_at`, `updated_at`
- Package fields: `platform`, `latest_size_bytes` (if present)

## Milestones
1) Load and render data
2) Basic faceting with Mosaic
3) Text search (baseline contains)
4) FTS prototype: DuckDB FTS
5) FTS prototype: FlexSearch
6) Compare and pick path

## Milestone 1: Load and Render
- Use Mosaic + DuckDB-WASM.
- Load Parquet into a DuckDB table (e.g., `worlds`).
- Render a minimal UI that shows a table with a small row limit.

Pseudo-steps:
- Initialize coordinator with `wasmConnector`.
- `loadParquet("worlds", "worlds_search.parquet")`.
- Add a `table` component: columns `name`, `author_name`, `visits`, `updated_at`.

## Milestone 2: Basic Faceting
Add facets and brushes:
- Histograms:
  - `visits` (log or linear)
  - `favorites`
  - `latest_size_bytes` (if present)
- Time brush:
  - `updated_at` histogram + interval selection
- Categorical menus:
  - `platform`
  - `author_name` (maybe top-k or search-backed)
- Tags:
  - Tag menu driven by `world_tags` or by unnesting `tags` in DuckDB.

Expected Mosaic components:
- `Selection.crossfilter()` for brushing.
- `menu`, `slider`, `search`, `table`.

## Milestone 3: Baseline Text Search
Use Mosaic `search` input with `type: "contains"`:
- Search on `name` or `author_name`.
- Confirm filters propagate through charts and table.

Stretch:
- Custom search input using `clauseMatchAny` on multiple columns.

## Milestone 4: DuckDB FTS Prototype
Goal: integrate DuckDB FTS extension with Mosaic selection.

Steps:
1) Install + load FTS:
   - `INSTALL fts;`
   - `LOAD fts;`
2) Build FTS index:
   - `PRAGMA create_fts_index('worlds', 'world_id', 'name', 'description', 'author_name');`
3) Search:
   - Use `fts_main_worlds.match_bm25(world_id, :query)` to get ranked ids.
4) Integration:
   - Create a search widget that emits a selection clause:
     - `world_id IN (SELECT world_id FROM fts_main_worlds.match_bm25(...))`
   - Verify it combines with other Mosaic filters.

Notes:
- FTS indexes don’t auto-update; recreate when data changes.
- Stemming/ranking limited but adequate for baseline.

## Milestone 5: FlexSearch Prototype
Goal: integrate a richer FTS index in the browser.

Steps:
1) Build FlexSearch index:
   - `Document` index with fields: `name`, `description`, `author_name`.
   - Store `world_id`.
   - Use worker mode for indexing to keep UI responsive.
2) Query:
   - Search returns `world_id` list (optionally with scores).
3) Integration options:
   - Option A: `world_id IN (...)` clause for small result lists.
   - Option B (preferred): load results into DuckDB temp table and join.
     - Create temp table `fts_results(world_id)` and populate per query.
     - Use predicate `world_id IN (SELECT world_id FROM fts_results)`.
4) Compare speed and relevance to DuckDB FTS.

## UI Layout Sketch
- Top row: search box + platform menu + tag menu.
- Middle row: visits histogram + favorites histogram + size histogram.
- Right panel: data table with key columns.
- Bottom: time brush on updated_at.

## Risks & Open Questions
- Loading time for large Parquet in browser.
- Memory usage with 210k rows + tags.
- Query performance with many facets and crossfilter.
- Whether DuckDB-WASM supports the FTS extension with our build.
- Best way to integrate FlexSearch results into Mosaic filters without large `IN` lists.

## Next Concrete Steps
1) Create a minimal Mosaic page that loads `worlds_search.parquet`.
2) Add a crossfilter selection + 2 histograms + table.
3) Add `search` input (contains) and verify linking.
4) Prototype DuckDB FTS in a branch.
5) Prototype FlexSearch in a branch and compare.

## Prototype App (Implemented)
Location: `web-prototype`

Setup:
1) `cd web-prototype`
2) `npm install`
3) `npm run dev`

Notes:
- The parquet file is served from `web-prototype/public/worlds_search.parquet`.
- UI uses Mosaic (vgplot) + DuckDB-WASM with a crossfilter selection.
