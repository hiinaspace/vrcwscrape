# Upload/metrics history extract (round 6)

Script: `history_extract.py <dolt> <dolt-repo-dir> <public-v2.parquet> <out-dir>`; run as
`uv run --no-project --with duckdb --with pymysql --with pyarrow python history_extract.py ...`.
Serves the repo with `dolt sql-server` on 127.0.0.1:13307, streams (world_id, file_metadata JSON) for
UNITY_PACKAGE/SUCCESS rows, parses in Python, writes `uploads.parquet`, `metrics.parquet`, `summary.parquet`, `asof.json`.
Runtime on a 17 GB repo copy: about 2 minutes. Disk: only the repo copy plus ~95 MB of output.

Contract: see round-6 brief. summary.parquet follows the row order of worlds-2026-10-20-public-v2.parquet
(verified 0 mismatches). Upload events: gaps <= 10 min between consecutive versions of a world (any platform)
chain into one event. 30/90/365d windows count from `asof` = max metrics scrape_time (2026-10-21 23:06:33 UTC).

Source/as-of: Dolt commit qdbfo4k47akfggtonav3m5jm6s5saupo (HEAD 2026-10-21 19:16:20 UTC = 13:16 MDT, same as the
public mirror HEAD). The live working tree ran ahead of that commit (data to 23:06 UTC); the copy was taken 17:09 MDT.

Package names use U+2024 "one dot leader" instead of "."; platform from `_android_|_standalonewindows_|_ios_`.

## Results (2026-10-21 run)
- uploads.parquet: 5,797,077 rows (5,797,913 version rows minus 10 without created_at minus 826 epoch-0 rows
  of wrld_350c0da4-4fa6-430f-b8f6-dd6a004771c4), 197,790 worlds; 65.7 MB. 74,681 non-public package rows skipped.
  Platforms: pc 4.41M, quest 1.29M, ios 104k, other 860. 291k rows have no size, 62.7k no unity_version, 2.86M flagged deleted.
- metrics.parquet: 1,772,916 rows, 197,992 worlds, 2025-06-24 .. 2026-10-21; 18.6 MB; per-world snapshots p50=10, p90=10, p99=13, max=262; occupants never null.
- summary.parquet: 197,992 rows, 7.3 MB. 202 worlds have no upload. uploads_total p50 8, p90 45, p99 172, max 3305;
  191 worlds > 500, 32 > 1000. Worlds with an upload in the last 30d: 3.81%, 90d: 10.79%, 365d: 30.07%.
- Top 10 by events: wrld_9d74f118-b1ae-4b2d-956e-e1ea32ca44e4 3305, wrld_cbb67b27-9442-45f9-b766-b0ed6d87001c 2647,
  wrld_5bcfd59f-2bf7-4de8-b264-b880a6ee3285 2579, wrld_02cb84c8-446a-484b-b52b-2c1777e55f8d 2254,
  wrld_ad903340-41c5-44a1-bf37-9bfad5c91a76 2093, wrld_4b341546-65ff-4607-9d38-5b7f8f405132 2051,
  wrld_ca7e5108-f81e-4cd0-bf8a-3b001fa860a9 1799, wrld_da9f25e6-1b1e-485c-8995-e791a02e8014 1789,
  wrld_44f4a344-4489-4e11-a54a-86971f16f0e6 1701, wrld_1281783a-d3ea-4a55-8d00-62c95b20cf66 1567.
- Pathological: wrld_350c0da4-... has 826 of 1326 versions with created_at = 1970-01-01 (dropped); 10 versions elsewhere lack created_at.
  Max raw versions in one world: 3793 (wrld_9d74f118). Uploads are not truncated.
- Spot checks vs raw JSON (version, created_at, size sets identical): Prismic's Avatar Search (453 rows; 3 packages,
  pc/quest 2019.4.31f1, ios 2022.3.22f1), wrld_9d74f118 (3793 rows), wrld_350c0da4 (500 valid of 1326).
