"""Database connection and operations for VRChat scraper."""

import json
from datetime import datetime
from typing import Dict, List
from urllib.parse import urlparse

import mysql.connector


class Database:
    """Database layer for VRChat world metadata storage."""

    def __init__(self, connection_string: str):
        """Initialize database connection."""
        self.connection_params = self._parse_connection_string(connection_string)

    def _parse_connection_string(self, connection_string: str) -> dict:
        """Parse MySQL connection string into parameters."""
        parsed = urlparse(connection_string)
        return {
            "host": parsed.hostname,
            "port": parsed.port or 3306,
            "user": parsed.username,
            "password": parsed.password,
            "database": parsed.path.lstrip("/"),
        }

    def _get_connection(self):
        """Create a new database connection."""
        return mysql.connector.connect(**self.connection_params)

    async def init_schema(self):
        """Create tables if they don't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Create worlds table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS worlds (
                    world_id VARCHAR(64) PRIMARY KEY,
                    metadata JSON NOT NULL,
                    publish_date DATETIME,
                    update_date DATETIME,
                    last_scrape_time DATETIME NOT NULL,
                    scrape_status ENUM('PENDING', 'SUCCESS', 'DELETED')
                )
            """)

            # Create metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS world_metrics (
                    world_id VARCHAR(64),
                    scrape_time DATETIME,
                    favorites INT,
                    heat INT,
                    popularity INT,
                    occupants INT,
                    private_occupants INT,
                    public_occupants INT,
                    visits INT,
                    PRIMARY KEY (world_id, scrape_time)
                )
            """)

            conn.commit()

    async def upsert_world(
        self, world_id: str, metadata: dict, status: str = "SUCCESS"
    ):
        """Insert or update world metadata."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO worlds (world_id, metadata, last_scrape_time, scrape_status)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                metadata = VALUES(metadata),
                last_scrape_time = VALUES(last_scrape_time),
                scrape_status = VALUES(scrape_status)
            """,
                (world_id, json.dumps(metadata), datetime.utcnow(), status),
            )
            conn.commit()

    async def insert_metrics(
        self, world_id: str, metrics: Dict[str, int], scrape_time: datetime
    ):
        """Insert world metrics for a specific time."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO world_metrics 
                (world_id, scrape_time, favorites, heat, popularity, occupants, 
                 private_occupants, public_occupants, visits)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    world_id,
                    scrape_time,
                    metrics["favorites"],
                    metrics["heat"],
                    metrics["popularity"],
                    metrics["occupants"],
                    metrics["private_occupants"],
                    metrics["public_occupants"],
                    metrics["visits"],
                ),
            )
            conn.commit()

    async def get_worlds_to_scrape(self, limit: int = 100) -> List[str]:
        """Get world IDs that need scraping based on strategy."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # First get PENDING worlds
            cursor.execute(
                """
                SELECT world_id FROM worlds 
                WHERE scrape_status = 'PENDING'
                ORDER BY RAND()
                LIMIT %s
            """,
                (limit,),
            )

            world_ids = [row[0] for row in cursor.fetchall()]

            if len(world_ids) < limit:
                # Get worlds that need rescraping based on age
                remaining = limit - len(world_ids)
                cursor.execute(
                    """
                    SELECT world_id, 
                           TIMESTAMPDIFF(DAY, publish_date, NOW()) as age_days,
                           TIMESTAMPDIFF(HOUR, last_scrape_time, NOW()) as hours_since_scrape
                    FROM worlds
                    WHERE scrape_status = 'SUCCESS'
                    HAVING 
                        (age_days < 7 AND hours_since_scrape >= 24) OR
                        (age_days < 30 AND hours_since_scrape >= 168) OR
                        (age_days < 365 AND hours_since_scrape >= 720) OR
                        (hours_since_scrape >= 8760)
                    ORDER BY RAND()
                    LIMIT %s
                """,
                    (remaining,),
                )

                world_ids.extend([row[0] for row in cursor.fetchall()])

            return world_ids
