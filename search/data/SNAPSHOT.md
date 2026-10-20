# Data snapshot 2026-10-20

Source: copy of the live Dolt DB taken 2026-10-19 23:48-23:52 MDT (scraper idle), dolt v1.81.2, served read-only
on 127.0.0.1:13307 (since stopped). ETL: `search/data/etl_search.py` (adapted from `scripts/etl.py`, which is untouched).

## Output files (cc-0, `/work/projects/vrcwscrape-data/`, not in git)
| file | rows | note |
|---|---|---|
| `worlds-2026-10-20.parquet` | 234,798 | every SUCCESS world (same rule as the June ETL), `release_status` column kept |
| `worlds-2026-10-20-public.parquet` | 197,992 | derived: `release_status` public (case-insensitive). **Recommended for the site.** |

Both are written to `*.part` then renamed, so a reader never sees a partial file. (An earlier intermediate
version of the first path held the 197,991 public-only rows; it was replaced by the full file at 00:28 MDT.)

## Why 234,798 vs 197,992 (the "37k gap")
Everything comes from `worlds.world_metadata->releaseStatus` among scrape_status = SUCCESS:
- `public`: 197,991; `Public` (odd casing): 1; `private`: **36,806**. Total 234,798 = the ~234.7-234.8k in the scraper logs.
- No duplicate world_ids (234,798 distinct), no SUCCESS row lacks JSON fields (name/tags/author/capacity null rate 0%).
- No other filter is applied: the 36,806 are private-release-status worlds (unlisted/unpublished, not joinable by
  others). The June parquet (218,097) did not filter them either, so they were silently in that file. For a public
  search site they are dead links, hence the separate public file; nothing public is dropped.

## Columns (June's 25 + 5)
world_id, name, description, author_id, author_name, capacity, created_at, updated_at, tags (VARCHAR[]), tag_count,
favorites, heat, popularity, occupants, private_occupants, public_occupants, visits, metrics_scrape_time,
pc_size_bytes, quest_size_bytes, pc_size_mb, quest_size_mb, thumb_sha256, scrape_status, last_scrape_time, plus
**thumbnail_url, image_url, platforms (VARCHAR[], values `pc`/`quest`/`ios`; union of the world JSON `unityPackages[].platform`
and the file_metadata package sizes), release_status**. ios sizes are not exported (no `ios_size_bytes` column).

## Sanity numbers
- created_at: min 2016-12-17, **max 2026-10-20 05:22 UTC** (=10-19 23:22 MDT); updated_at max 2026-10-20 05:31 UTC;
  metrics_scrape_time range 2026-04-23 .. 2026-10-20 (latest metrics row per world, so stale for cold worlds).
- worlds created since 2026-06-01: 17,961 (public file).
- tags: `VARCHAR[]`, never null; tag_count = 0 for 8 worlds. platforms: never null/empty.
- Null rates, all rows / public only: pc_size 0.82% / 0.77%, quest_size 57.81% / 57.32% (no Quest build; consistent with
  platforms), thumb_sha256 0.62% / 0.61%; every other column 0%. 94 worlds have an empty name.
- Platforms (public): pc 112,828; pc+quest 74,721; ios+pc+quest 9,397; quest-only 1,001; ios+pc 35; ios+quest 7; ios 3.
  523 worlds list `pc` in unityPackages but have no pc file_metadata size; 616 likewise for quest.
- ETL runtime ~5-6 min (the package-size query via PyMySQL dominates).
- Not checked: thumbnail_url validity (URLs are the `/image/<file>/<ver>/256` form, no network fetch done).
