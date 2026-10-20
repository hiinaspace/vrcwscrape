#!/usr/bin/env python3
"""Recent popularity: visits/day over the most recent ~30d window, from world_metrics (append-only series).

Pulls only (world_id, scrape_time, visits) from the read-only Dolt copy (no JSON scanning; ~2M rows, seconds),
computes the window in DuckDB, and writes <public parquet> + recent_* columns (same rows and order).

Window rule per world: latest snapshot t1; reference = latest snapshot with t <= t1 - 30d (may be much older than
30d -> recorded in recent_window_days); if none exists, the earliest snapshot at least 1 day before t1 (shorter
window). <2 usable snapshots -> recent_visits_per_day null, recent_basis = popularity_fallback.
Negative visit deltas (counter corrections) are clipped to 0.

Usage: recent_popularity.py --database-url mysql://root@127.0.0.1:13307/vrcwscrape --input public.parquet --output v2.parquet
"""
import argparse
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import pandas as pd
import pymysql


def fetch_metrics(url: str) -> pd.DataFrame:
    p = urlparse(url)
    my = pymysql.connect(host=p.hostname or "localhost", port=p.port or 3306, user=p.username or "root",
                         password=p.password or "", database=p.path.lstrip("/") or "vrcwscrape")
    try:
        cur = my.cursor()
        cur.execute("SELECT world_id, scrape_time, visits FROM world_metrics")
        return pd.DataFrame(cur.fetchall(), columns=["world_id", "scrape_time", "visits"])
    finally:
        my.close()


def run(url: str, inp: Path, out: Path, window_days: int = 30) -> None:
    m = fetch_metrics(url)
    print(f"fetched {len(m):,} metric rows, {m.world_id.nunique():,} worlds")
    con = duckdb.connect()
    con.register("m", m)
    con.execute(f"""
    CREATE TABLE rec AS
    WITH ranked AS (
      SELECT *, row_number() OVER (PARTITION BY world_id ORDER BY scrape_time DESC) rn_desc,
             max(scrape_time) OVER (PARTITION BY world_id) t1
      FROM m),
    latest AS (SELECT world_id, scrape_time t1, visits v1 FROM ranked WHERE rn_desc = 1),
    ref_before AS (
      SELECT world_id, scrape_time t0, visits v0 FROM (
        SELECT *, row_number() OVER (PARTITION BY world_id ORDER BY scrape_time DESC) r
        FROM ranked WHERE scrape_time <= t1 - INTERVAL {window_days} DAY) WHERE r = 1),
    ref_first AS (
      SELECT world_id, scrape_time t0, visits v0 FROM (
        SELECT *, row_number() OVER (PARTITION BY world_id ORDER BY scrape_time ASC) r
        FROM ranked WHERE scrape_time <= t1 - INTERVAL 1 DAY) WHERE r = 1),
    j AS (
      SELECT l.world_id, l.t1, l.v1, COALESCE(b.t0, f.t0) t0, COALESCE(b.v0, f.v0) v0
      FROM latest l LEFT JOIN ref_before b USING (world_id) LEFT JOIN ref_first f USING (world_id))
    SELECT world_id,
      CASE WHEN t0 IS NULL THEN NULL
           ELSE CAST(greatest(v1 - v0, 0) / (epoch(t1 - t0) / 86400.0) AS FLOAT) END AS recent_visits_per_day,
      CASE WHEN t0 IS NULL THEN NULL ELSE CAST(epoch(t1 - t0) / 86400.0 AS FLOAT) END AS recent_window_days,
      CASE WHEN t0 IS NULL THEN 'popularity_fallback' ELSE 'metrics' END AS recent_basis
    FROM j""")
    part = out.with_name(out.name + ".part")
    con.execute(f"""COPY (SELECT w.* EXCLUDE (_o), r.recent_visits_per_day, r.recent_window_days,
                            COALESCE(r.recent_basis, 'popularity_fallback') AS recent_basis
                     FROM (SELECT *, row_number() OVER () AS _o FROM read_parquet('{inp}')) w
                     LEFT JOIN rec r USING (world_id) ORDER BY w._o)
                   TO '{part}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    part.rename(out)
    print("wrote", out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default="mysql://root@127.0.0.1:13307/vrcwscrape")
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--window-days", type=int, default=30)
    a = ap.parse_args()
    run(a.database_url, a.input, a.output, a.window_days)
