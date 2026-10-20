#!/usr/bin/env python3
"""Merge per-batch parts (sha256, avif, width, height) into thumbs.parquet (world_id, sha256, avif, width, height).

One row per public world with an image; worlds sharing a thumb_sha256 repeat the same avif bytes (so a pack
builder should dedupe on sha256). Also writes thumbs-by-sha.parquet (unique sha rows) and thumbs-report.txt.
"""
import sys
from pathlib import Path

import duckdb

D = Path("/work/projects/vrcwscrape-data")
con = duckdb.connect()
parts = f"{D}/thumbs/parts/part-*.parquet"
pub = f"{D}/worlds-2026-10-20-public-v2.parquet"
con.execute(f"CREATE VIEW p AS SELECT * FROM read_parquet('{parts}')")
for name, sql in {
    "thumbs-by-sha.parquet": "SELECT * FROM p ORDER BY sha256",
    "thumbs.parquet": f"""SELECT w.world_id, w.thumb_sha256 AS sha256, p.avif, p.width, p.height
        FROM (SELECT world_id, thumb_sha256 FROM read_parquet('{pub}') WHERE thumb_sha256 IS NOT NULL) w
        JOIN p ON p.sha256 = w.thumb_sha256 ORDER BY w.world_id""",
}.items():
    out = D / "thumbs" / name
    part = out.with_name(out.name + ".part")
    con.execute(f"COPY ({sql}) TO '{part}' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 5000)")
    part.rename(out)
r = con.execute(f"""SELECT
  (SELECT count(*) FROM read_parquet('{pub}')),
  (SELECT count(*) FROM read_parquet('{pub}') WHERE thumb_sha256 IS NOT NULL),
  (SELECT count(DISTINCT thumb_sha256) FROM read_parquet('{pub}')),
  (SELECT count(*) FROM p), (SELECT count(*) FROM read_parquet('{D}/thumbs/thumbs.parquet')),
  (SELECT avg(length(avif)) FROM p), (SELECT quantile_cont(length(avif),0.95) FROM p),
  (SELECT sum(length(avif)) FROM p)""").fetchone()
print(r)
