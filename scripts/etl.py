#!/usr/bin/env python3
"""ETL pipeline to export VRChat world data from DoltDB to Parquet for analytics.

Uses a hybrid approach:
- DuckDB's MySQL extension for most queries (worlds, tags, metrics)
- PyMySQL directly for file_metadata queries with complex JSON structures,
  as DoltDB doesn't efficiently handle JSON operations through DuckDB's extension

The file_metadata table contains large JSON blobs (up to 800KB) with arrays of
1000+ versions per world. Extracting only the latest version at the MySQL layer
provides a ~100x speedup compared to unpacking all versions through DuckDB.
"""

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import pymysql
from dotenv import load_dotenv


def build_mysql_attach_string(database_url: str) -> str:
    """Convert a MySQL URL to DuckDB ATTACH connection string."""
    parsed = urlparse(database_url)
    parts = [
        f"host={parsed.hostname or 'localhost'}",
        f"user={parsed.username or 'root'}",
        f"port={parsed.port or 3306}",
        f"database={parsed.path.lstrip('/') if parsed.path else 'vrcworld'}",
    ]
    if parsed.password:
        parts.append(f"password={parsed.password}")
    return " ".join(parts)


def connect_duckdb_to_mysql(database_url: str) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with MySQL attached."""
    conn = duckdb.connect()
    conn.execute("LOAD mysql")
    attach_str = build_mysql_attach_string(database_url)
    conn.execute(f"ATTACH '{attach_str}' AS mysqldb (TYPE mysql)")
    return conn


@dataclass
class ExportLimits:
    """Optional limits for faster, smaller exports."""

    worlds_limit: int | None = None
    packages_limit: int | None = None


def _limit_clause(limit: int | None) -> str:
    if limit is None:
        return ""
    return f"LIMIT {int(limit)}"


def _ensure_selected_worlds(
    conn: duckdb.DuckDBPyConnection, worlds_limit: int | None
) -> bool:
    if not worlds_limit:
        return False
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE selected_worlds AS
        SELECT world_id
        FROM mysqldb.worlds
        WHERE scrape_status = 'SUCCESS'
        ORDER BY last_scrape_time DESC
        LIMIT {int(worlds_limit)}
    """)
    return True


def _export_unity_packages(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    use_selected_worlds: bool,
    limit: int | None,
    database_url: str,
    output_name: str = "unity_packages_temp.parquet",
) -> int:
    """Export latest unity package size from file_metadata to a temp parquet.

    Performance optimization: Uses PyMySQL directly with MySQL's native JSON
    functions to extract only the LAST version from each world's file_metadata,
    avoiding the need to unpack thousands of versions through DuckDB's MySQL
    extension which doesn't push down JSON operations efficiently to DoltDB.

    Strategy:
    1. Connect directly to MySQL/DoltDB via PyMySQL
    2. Use JSON_EXTRACT with dynamic array indexing to get only the last version
       (versions array can have 1000+ entries for long-lived worlds)
    3. Process platform detection in Python (fast for small result set)
    4. Write to CSV and convert to Parquet via DuckDB

    This approach is ~100x faster than the previous implementation that used
    DuckDB's mysql extension with CROSS JOIN json_each() over all versions.
    """
    print("Exporting unity packages (temp)...")

    output_path = output_dir / output_name
    csv_path = output_dir / "unity_packages_temp.csv"

    # Parse database URL
    parsed = urlparse(database_url)

    # Connect directly to MySQL with PyMySQL for efficient JSON queries
    mysql_conn = pymysql.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 3306,
        user=parsed.username or "root",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") if parsed.path else "vrcwscrape",
    )

    try:
        cursor = mysql_conn.cursor()

        # Build query with optional filtering
        where_clauses = [
            "fm.scrape_status = 'SUCCESS'",
            "fm.file_type != 'IMAGE'",
            "fm.file_metadata IS NOT NULL",
        ]

        if use_selected_worlds:
            # Get selected world IDs from DuckDB temp table
            selected_ids = conn.execute(
                "SELECT world_id FROM selected_worlds"
            ).fetchall()
            world_id_list = ", ".join(f"'{wid[0]}'" for wid in selected_ids)
            where_clauses.append(f"fm.world_id IN ({world_id_list})")

        where_sql = " AND ".join(where_clauses)
        limit_clause = _limit_clause(limit)

        # Use MySQL's native JSON functions to extract only the LAST version
        # This avoids unpacking thousands of versions per world
        query = f"""
            SELECT
                fm.file_id,
                fm.world_id,
                LOWER(JSON_EXTRACT(fm.file_metadata, '$.name')) as name,
                JSON_EXTRACT(
                    fm.file_metadata,
                    CONCAT('$.versions[', JSON_LENGTH(fm.file_metadata, '$.versions') - 1, '].version')
                ) as latest_version,
                JSON_EXTRACT(
                    fm.file_metadata,
                    CONCAT('$.versions[', JSON_LENGTH(fm.file_metadata, '$.versions') - 1, '].file.sizeInBytes')
                ) as latest_size_bytes
            FROM file_metadata fm
            WHERE {where_sql}
              AND JSON_LENGTH(fm.file_metadata, '$.versions') > 0
            {limit_clause}
        """

        print("  Querying MySQL with native JSON functions...")
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()

        print(f"  Fetched {len(rows):,} rows from MySQL")

        # Write to CSV for DuckDB to process
        import csv

        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "file_id",
                    "world_id",
                    "platform",
                    "latest_version",
                    "latest_size_bytes",
                ]
            )
            for row in rows:
                file_id, world_id, name, version, size_bytes = row
                # Determine platform from name field (contains Unity asset bundle info)
                name_lower = (name or "").strip('"').lower()
                if "standalonewindows" in name_lower or "_pc_" in name_lower:
                    platform = "standalonewindows"
                elif "android" in name_lower or "_quest_" in name_lower:
                    platform = "android"
                else:
                    platform = "unknown"

                # Write row with platform classification
                writer.writerow([file_id, world_id, platform, version, size_bytes])

        # Convert CSV to Parquet using DuckDB
        conn.execute(f"""
            COPY (
                SELECT
                    file_id,
                    world_id,
                    CAST(platform AS VARCHAR) as platform,
                    CAST(latest_version AS INT) as latest_version,
                    CAST(latest_size_bytes AS BIGINT) as latest_size_bytes
                FROM read_csv_auto('{csv_path}')
                WHERE latest_size_bytes IS NOT NULL
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        # Clean up temp CSV
        csv_path.unlink()

    finally:
        mysql_conn.close()

    row_count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    ).fetchone()[0]

    print(f"  Wrote {row_count:,} unity packages to {output_path}")
    return row_count


def export_worlds_search(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    limits: ExportLimits,
    database_url: str,
    use_selected_worlds: bool,
) -> int:
    """Export a single denormalized parquet for fast search/UI faceting."""
    print("Exporting worlds_search...")

    output_path = output_dir / "worlds_search.parquet"
    packages_path = output_dir / "unity_packages_temp.parquet"
    packages_limit = limits.packages_limit

    if packages_path.exists():
        packages_path.unlink()

    _export_unity_packages(
        conn,
        output_dir,
        use_selected_worlds,
        packages_limit,
        database_url,
        output_name=packages_path.name,
    )

    world_source = "mysqldb.worlds w"
    if use_selected_worlds:
        world_source = (
            "mysqldb.worlds w JOIN selected_worlds s ON w.world_id = s.world_id"
        )

    limit_clause = _limit_clause(limits.worlds_limit)
    order_clause = "ORDER BY w.last_scrape_time DESC" if limits.worlds_limit else ""

    conn.execute(f"""
        COPY (
            WITH latest_metrics AS (
                SELECT
                    world_id,
                    scrape_time,
                    favorites,
                    heat,
                    popularity,
                    occupants,
                    private_occupants,
                    public_occupants,
                    visits
                FROM (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY world_id
                            ORDER BY scrape_time DESC
                        ) AS rn
                    FROM mysqldb.world_metrics
                )
                WHERE rn = 1
            ),
            package_sizes AS (
                SELECT
                    world_id,
                    MAX(CASE WHEN platform = 'standalonewindows' THEN latest_size_bytes END) AS pc_size_bytes,
                    MAX(CASE WHEN platform = 'android' THEN latest_size_bytes END) AS quest_size_bytes
                FROM read_parquet('{packages_path}')
                GROUP BY world_id
            )
            SELECT
                w.world_id,
                w.world_metadata::JSON->>'$.name' AS name,
                w.world_metadata::JSON->>'$.description' AS description,
                w.world_metadata::JSON->>'$.authorId' AS author_id,
                w.world_metadata::JSON->>'$.authorName' AS author_name,
                CAST(w.world_metadata::JSON->>'$.capacity' AS INT) AS capacity,
                COALESCE(
                    w.publish_date,
                    try_cast(w.world_metadata::JSON->>'$.created_at' AS TIMESTAMP)
                ) AS created_at,
                COALESCE(
                    w.update_date,
                    try_cast(w.world_metadata::JSON->>'$.updated_at' AS TIMESTAMP)
                ) AS updated_at,
                CAST(w.world_metadata::JSON->'$.tags' AS VARCHAR[]) AS tags,
                COALESCE(json_array_length(w.world_metadata::JSON->'$.tags'), 0) AS tag_count,
                lm.favorites,
                lm.heat,
                lm.popularity,
                lm.occupants,
                lm.private_occupants,
                lm.public_occupants,
                lm.visits,
                lm.scrape_time AS metrics_scrape_time,
                ps.pc_size_bytes,
                ps.quest_size_bytes,
                CAST(ps.pc_size_bytes AS DOUBLE) / 1024 / 1024 AS pc_size_mb,
                CAST(ps.quest_size_bytes AS DOUBLE) / 1024 / 1024 AS quest_size_mb,
                w.scrape_status,
                w.last_scrape_time
            FROM {world_source}
            LEFT JOIN latest_metrics lm ON w.world_id = lm.world_id
            LEFT JOIN package_sizes ps ON w.world_id = ps.world_id
            WHERE w.scrape_status = 'SUCCESS'
            {order_clause}
            {limit_clause}
        ) TO '{output_path}' (FORMAT PARQUET)
    """)

    if packages_path.exists():
        packages_path.unlink()

    row_count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    ).fetchone()[0]
    print(f"  Wrote {row_count:,} rows to {output_path}")
    return row_count


def run_etl(
    database_url: str,
    output_dir: Path,
    limits: ExportLimits | None = None,
) -> dict[str, int]:
    """Run the ETL pipeline for a single denormalized parquet."""
    if limits is None:
        limits = ExportLimits()

    output_dir.mkdir(parents=True, exist_ok=True)

    print("Connecting to database via DuckDB MySQL extension...")
    conn = connect_duckdb_to_mysql(database_url)

    results = {}
    start_time = datetime.now()

    try:
        use_selected_worlds = _ensure_selected_worlds(conn, limits.worlds_limit)

        results["worlds_search"] = export_worlds_search(
            conn,
            output_dir,
            limits,
            database_url,
            use_selected_worlds,
        )

    finally:
        conn.close()

    elapsed = datetime.now() - start_time
    print(f"\nETL completed in {elapsed.total_seconds():.1f}s")

    return results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Export VRChat world data from DoltDB to a single Parquet file"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./analytics"),
        help="Output directory for parquet file (default: ./analytics)",
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default=None,
        help="MySQL connection URL (default: from DATABASE_URL env var)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Run a limited export for faster iteration "
            "(defaults to --worlds-limit 1000 --packages-limit 1000)."
        ),
    )
    parser.add_argument(
        "--worlds-limit",
        type=int,
        default=None,
        help="Limit number of worlds exported (also limits tags/metrics/packages).",
    )
    parser.add_argument(
        "--packages-limit",
        type=int,
        default=None,
        help="Limit number of unity package rows exported.",
    )

    args = parser.parse_args()

    load_dotenv()

    database_url = args.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        print(
            "Error: DATABASE_URL not set. Use --database-url or set DATABASE_URL env var."
        )
        sys.exit(1)

    try:
        limits = ExportLimits(
            worlds_limit=args.worlds_limit,
            packages_limit=args.packages_limit,
        )
        if args.test:
            if limits.worlds_limit is None:
                limits.worlds_limit = 1000
            if limits.packages_limit is None:
                limits.packages_limit = 1000

        results = run_etl(database_url, args.output_dir, limits)

        print("\nSummary:")
        for table, count in results.items():
            print(f"  {table}: {count:,} rows")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
