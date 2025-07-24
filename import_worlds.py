#!/usr/bin/env python3
"""Import world IDs from stdin into database as pending worlds.

Usage:
    cat world_ids.txt | uv run python import_worlds.py
    uv run python import_worlds.py < world_ids.txt
"""

import asyncio
import sys
from datetime import datetime
from typing import List, Tuple

from src.vrchat_scraper.config import Config
from src.vrchat_scraper.database import Database, ScrapeStatus


async def import_worlds_from_stdin():
    """Read world IDs from stdin and import them as pending worlds."""
    # Load configuration
    config = Config()

    # Initialize database
    db = Database(config.database_url)
    await db.init_schema()

    # Read world IDs from stdin
    world_ids = []
    for line in sys.stdin:
        world_id = line.strip()
        if world_id and world_id.startswith("wrld_"):
            world_ids.append(world_id)

    if not world_ids:
        print("No valid world IDs found in input", file=sys.stderr)
        return

    print(f"Found {len(world_ids)} world IDs to import")

    # Process in batches to avoid overwhelming the database
    batch_size = 1000
    total_imported = 0
    total_skipped = 0

    for i in range(0, len(world_ids), batch_size):
        batch = world_ids[i : i + batch_size]
        imported, skipped = await import_batch(db, batch)
        total_imported += imported
        total_skipped += skipped

        print(
            f"Processed batch {i // batch_size + 1}: {imported} imported, {skipped} skipped"
        )

    print(
        f"Import complete: {total_imported} worlds imported, {total_skipped} already existed"
    )


async def import_batch(db: Database, world_ids: List[str]) -> Tuple[int, int]:
    """Import a batch of world IDs, returning counts of imported and skipped."""
    # Check which worlds already exist
    async with db.async_session() as session:
        from sqlalchemy import select
        from src.vrchat_scraper.database import World

        existing_result = await session.execute(
            select(World.world_id).where(World.world_id.in_(world_ids))
        )
        existing_world_ids = {row[0] for row in existing_result.fetchall()}

    # Prepare batch data for worlds that don't exist
    worlds_to_import = []
    for world_id in world_ids:
        if world_id not in existing_world_ids:
            worlds_to_import.append(
                (
                    world_id,
                    {"imported_at": datetime.utcnow().isoformat()},
                    ScrapeStatus.PENDING.value,
                )
            )

    # Import new worlds
    if worlds_to_import:
        await db.batch_upsert_worlds(worlds_to_import)

    imported_count = len(worlds_to_import)
    skipped_count = len(world_ids) - imported_count

    return imported_count, skipped_count


if __name__ == "__main__":
    asyncio.run(import_worlds_from_stdin())
