# Self-hosted thumbnails + recent popularity (round 5, data lane)

## Recent popularity (`recent_popularity.py`)
Adds `recent_visits_per_day`, `recent_window_days`, `recent_basis` to the public parquet -> `worlds-2026-10-20-public-v2.parquet`
(same rows/order). Source: `world_metrics` (2.16M rows, 249,858 worlds, 2025-06-24..2026-10-20), pulled as
(world_id, scrape_time, visits) only -- no JSON scan, ~seconds. Window rule: latest snapshot t1; reference = latest
snapshot at or before t1-30d (may be older; the real window is recorded in `recent_window_days`); if none, the earliest
snapshot >= 1 day before t1. <2 usable snapshots -> null + `popularity_fallback`. Negative deltas are clipped to 0.
Needs the read-only Dolt copy served on 127.0.0.1:13307.

## Thumbnails
`thumb_sha256` in the parquet is `image_content.sha256` of the world's latest CONFIRMED IMAGE file version: the
**full-size** world image PNG (800x600..1200x900), stored on the scraper host at
`/mnt/nvme/vrcw-scrape/images/<aa>/<bb>/<sha256>.png` (aa/bb = chars 1-2 / 3-4). Several worlds share a sha.

Pipeline (never encodes on chirashi, which is the live scraper): `thumbs_driver.py` (run on natto with ssh agent
forwarding) -> on chirashi `nice -n19 ionice -c3 tar -cf - -T -` (read only, nothing written) -> `ssh claude@cc.hl` ->
`encode_stream.py` on cc-0 (reader throttled to 20 MB/s average, which back-pressures tar; 5 worker processes) ->
`parts/part-NNNN.parquet` (10k unique shas per batch, resumable: existing parts are skipped). Between batches the
driver logs chirashi load/swap/IO pressure/scraper unit state to `thumbs/run/health.log` and stops if the unit is `failed`.
`merge_thumbs.py` joins parts to worlds -> `thumbs.parquet` (world_id, sha256, avif, width, height) plus
`thumbs-by-sha.parquet` (unique images; pack builders should dedupe on sha256).

Encoder: Pillow 13 native AVIF, LANCZOS resize to 256 px wide (height keeps aspect, mostly 192), RGB, quality 50, speed 4.
Pilot (500 random images): avg 3.6 KB, p95 6.6 KB; q30 averaged 2.2 KB and q70 6.2 KB on the contact-sheet sample
(`thumbs/pilot/contact_q{30,50,70}.png`). Missing images (no file in store) are listed per batch in `*.missing.txt`.

## Result (run 2026-10-20 15:49..20:57 MDT)
- 193,235 unique shas encoded, 0 missing in the store, 0 decode errors. 196,778 of 197,992 public worlds have a thumb
  (the other 1,214 = 0.61% have no `thumb_sha256` in the DB; they get the placeholder).
- Size: avg 3.59 KB, median 3.4 KB, p95 6.6 KB, p99 8.3 KB, max 250 KB (1 outlier); 699 MB unique
  (`thumbs-by-sha.parquet`), 706 MB per-world (`thumbs.parquet`, 3.5k worlds repeat a shared image; dedupe on sha256).
- Dimensions: 256 x 192 for most; 800 images have another height (non-4:3 originals), max 455.
- Rules in the driver: streaming pauses (SIGSTOP of our own tar PID) while vrcwscrape.service is `active`/`activating`;
  `inactive` and `failed` are idle. Stops if a run fails with a reason other than the known `timeout` shutdown hang, or
  5 of the last 6 runs fail. Throttle has no catch-up credit after pauses. Wall time is dominated by pauses.
- Scraper impact (journal): 128 runs before the stream, 41% `timeout` failures, median successful run 12.6 min;
  during the stream 30% failures, median 12.8 min. No change observed.
