#!/usr/bin/env python3
"""ETL for the static search site: Dolt (read-only snapshot) -> one denormalised parquet.

Adapted from scripts/etl.py (which is left untouched). Same 25 columns as the June
mapgen/data/worlds_search.parquet, plus: world_id (already col 0 there, kept), thumbnail_url,
image_url, platforms (list of 'pc'|'quest'|'ios'), release_status.

Usage:
  etl_search.py --database-url mysql://root@127.0.0.1:13307/vrcwscrape --output worlds.parquet

All scrape_status = SUCCESS worlds are exported (like the June ETL), with release_status kept as a
column. ~36.8k of ~234.8k are releaseStatus=private (unjoinable); a second file with only
public rows is derived next to the output (<name>-public.parquet) -- the site should use that one. worlds are exported.
"""

import argparse
import csv
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import pymysql


def attach_string(url: str) -> str:
    p = urlparse(url)
    parts = [
        f"host={p.hostname or 'localhost'}",
        f"user={p.username or 'root'}",
        f"port={p.port or 3306}",
        f"database={p.path.lstrip('/') or 'vrcwscrape'}",
    ]
    if p.password:
        parts.append(f"password={p.password}")
    return " ".join(parts)


def export_packages(conn: duckdb.DuckDBPyConnection, url: str, out: Path, tmp: Path) -> int:
    """Latest unity package size per (world, platform), via native MySQL JSON functions
    (dolt is slow at unpacking 1000+ version arrays through DuckDB's scanner)."""
    p = urlparse(url)
    my = pymysql.connect(
        host=p.hostname or "localhost",
        port=p.port or 3306,
        user=p.username or "root",
        password=p.password or "",
        database=p.path.lstrip("/") or "vrcwscrape",
    )
    try:
        cur = my.cursor()
        cur.execute("""
            SELECT fm.file_id, fm.world_id,
                   LOWER(JSON_EXTRACT(fm.file_metadata, '$.name')),
                   JSON_EXTRACT(fm.file_metadata,
                     CONCAT('$.versions[', JSON_LENGTH(fm.file_metadata, '$.versions') - 1, '].version')),
                   JSON_EXTRACT(fm.file_metadata,
                     CONCAT('$.versions[', JSON_LENGTH(fm.file_metadata, '$.versions') - 1, '].file.sizeInBytes'))
            FROM file_metadata fm
            WHERE fm.scrape_status = 'SUCCESS' AND fm.file_type != 'IMAGE'
              AND fm.file_metadata IS NOT NULL
              AND JSON_LENGTH(fm.file_metadata, '$.versions') > 0
        """)
        rows = cur.fetchall()
    finally:
        my.close()
    print(f"  fetched {len(rows):,} package rows")
    csv_path = tmp / "pk.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file_id", "world_id", "platform", "latest_version", "latest_size_bytes"])
        for file_id, world_id, name, ver, size in rows:
            n = (name or "").strip('"').lower()
            if "standalonewindows" in n or "_pc_" in n:
                plat = "standalonewindows"
            elif "android" in n or "_quest_" in n:
                plat = "android"
            elif "ios" in n:
                plat = "ios"
            else:
                plat = "unknown"
            w.writerow([file_id, world_id, plat, ver, size])
    conn.execute(f"""
        COPY (SELECT file_id, world_id, CAST(platform AS VARCHAR) platform,
                     CAST(latest_version AS INT) latest_version,
                     CAST(latest_size_bytes AS BIGINT) latest_size_bytes
              FROM read_csv_auto('{csv_path}', all_varchar=true)
              WHERE latest_size_bytes IS NOT NULL) TO '{out}' (FORMAT PARQUET)""")
    csv_path.unlink()
    return len(rows)


def run(url: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    conn.execute("INSTALL mysql; LOAD mysql")
    conn.execute(f"ATTACH '{attach_string(url)}' AS mysqldb (TYPE mysql)")
    t0 = datetime.now()
    with tempfile.TemporaryDirectory(dir=output.parent) as td:
        tmp = Path(td)
        pk = tmp / "packages.parquet"
        export_packages(conn, url, pk, tmp)
        # NB: wm.* JSON read once per world; the CTE keeps JSON parsing in DuckDB.
        conn.execute(f"""
        COPY (
          WITH latest_metrics AS (
            SELECT world_id, scrape_time, favorites, heat, popularity, occupants,
                   private_occupants, public_occupants, visits
            FROM (SELECT *, row_number() OVER (PARTITION BY world_id ORDER BY scrape_time DESC) rn
                  FROM mysqldb.world_metrics) WHERE rn = 1),
          package_sizes AS (
            SELECT world_id,
              MAX(CASE WHEN platform='standalonewindows' THEN latest_size_bytes END) pc_size_bytes,
              MAX(CASE WHEN platform='android' THEN latest_size_bytes END) quest_size_bytes,
              MAX(CASE WHEN platform='ios' THEN latest_size_bytes END) ios_size_bytes
            FROM read_parquet('{pk}') GROUP BY world_id),
          thumbnails AS (
            SELECT world_id, thumb_sha256 FROM (
              SELECT fm.world_id, ic.sha256 thumb_sha256,
                     row_number() OVER (PARTITION BY fm.world_id ORDER BY ic.version DESC) rn
              FROM mysqldb.file_metadata fm JOIN mysqldb.image_content ic ON fm.file_id = ic.file_id
              WHERE fm.file_type='IMAGE' AND ic.state='CONFIRMED' AND ic.sha256 IS NOT NULL)
            WHERE rn = 1),
          src AS (
            SELECT w.world_id, w.world_metadata::JSON j, w.publish_date, w.update_date,
                   w.scrape_status, w.last_scrape_time
            FROM mysqldb.worlds w WHERE w.scrape_status = 'SUCCESS')
          SELECT
            s.world_id,
            s.j->>'$.name' AS name,
            s.j->>'$.description' AS description,
            s.j->>'$.authorId' AS author_id,
            s.j->>'$.authorName' AS author_name,
            CAST(s.j->>'$.capacity' AS INT) AS capacity,
            COALESCE(s.publish_date, try_cast(s.j->>'$.created_at' AS TIMESTAMP)) AS created_at,
            COALESCE(s.update_date, try_cast(s.j->>'$.updated_at' AS TIMESTAMP)) AS updated_at,
            CAST(COALESCE(s.j->'$.tags', '[]'::JSON) AS VARCHAR[]) AS tags,
            COALESCE(json_array_length(s.j->'$.tags'), 0) AS tag_count,
            lm.favorites, lm.heat, lm.popularity, lm.occupants, lm.private_occupants,
            lm.public_occupants, lm.visits, lm.scrape_time AS metrics_scrape_time,
            ps.pc_size_bytes, ps.quest_size_bytes,
            CAST(ps.pc_size_bytes AS DOUBLE)/1024/1024 AS pc_size_mb,
            CAST(ps.quest_size_bytes AS DOUBLE)/1024/1024 AS quest_size_mb,
            th.thumb_sha256, s.scrape_status, s.last_scrape_time,
            s.j->>'$.thumbnailImageUrl' AS thumbnail_url,
            s.j->>'$.imageUrl' AS image_url,
            list_sort(list_distinct(list_filter([
              CASE WHEN ps.pc_size_bytes IS NOT NULL THEN 'pc' END,
              CASE WHEN ps.quest_size_bytes IS NOT NULL THEN 'quest' END,
              CASE WHEN ps.ios_size_bytes IS NOT NULL THEN 'ios' END]
              || list_transform(
                   CAST(COALESCE(json_extract(s.j,'$.unityPackages[*].platform'),'[]'::JSON) AS VARCHAR[]),
                   x -> CASE trim(x,'"') WHEN 'standalonewindows' THEN 'pc'
                                         WHEN 'android' THEN 'quest' WHEN 'ios' THEN 'ios' END),
              x -> x IS NOT NULL))) AS platforms,
            s.j->>'$.releaseStatus' AS release_status
          FROM src s
          LEFT JOIN latest_metrics lm ON s.world_id = lm.world_id
          LEFT JOIN package_sizes ps ON s.world_id = ps.world_id
          LEFT JOIN thumbnails th ON s.world_id = th.world_id
        ) TO '{output}.part' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    # derived public-only file; atomic renames so readers never see a partial write
    part = Path(f"{output}.part")
    pub = output.with_name(output.stem + "-public.parquet")
    ptmp = pub.with_name(pub.name + ".part")
    conn.execute(
        f"COPY (SELECT * FROM read_parquet('{part}') WHERE lower(COALESCE(release_status,'public'))='public') "
        f"TO '{ptmp}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    part.rename(output)
    ptmp.rename(pub)
    n = conn.execute(f"SELECT count(*) FROM read_parquet('{output}')").fetchone()[0]
    print(f"wrote {n:,} rows -> {output} in {(datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default="mysql://root@127.0.0.1:13307/vrcwscrape")
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    run(a.database_url, a.output)
