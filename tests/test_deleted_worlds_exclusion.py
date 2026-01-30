"""Test that DELETED worlds are properly excluded from scraping."""

import pytest
import pytest_asyncio

from src.vrchat_scraper.database import Database


@pytest_asyncio.fixture
async def test_db():
    """Create an in-memory SQLite database for testing."""
    db = Database("sqlite:///:memory:")
    await db.init_schema()
    return db


@pytest.mark.asyncio
async def test_deleted_worlds_excluded_from_count(test_db):
    """Test that DELETED worlds are excluded from count_worlds_due_for_scraping."""
    # Insert worlds with different statuses
    await test_db.upsert_world("wrld_pending_1", {"name": "Pending 1"}, "PENDING")
    await test_db.upsert_world("wrld_deleted_1", {"name": "Deleted 1"}, "DELETED")
    await test_db.upsert_world("wrld_deleted_2", {"name": "Deleted 2"}, "DELETED")

    # Create a SUCCESS world with recent metrics
    await test_db.upsert_world_with_files(
        "wrld_success_1",
        {"name": "Success 1"},
        {
            "heat": 5,
            "popularity": 5,
            "occupants": 1,
            "private_occupants": 0,
            "public_occupants": 1,
            "favorites": 10,
            "visits": 100,
        },
        [],
        "SUCCESS",
    )

    counts = await test_db.count_worlds_due_for_scraping()

    # Should only count PENDING world, not DELETED ones
    assert counts["pending"] == 1
    assert counts["deleted"] == 2
    assert counts["success"] == 1
    assert counts["total"] == 1  # Only PENDING world


@pytest.mark.asyncio
async def test_deleted_worlds_excluded_from_get_worlds_to_scrape(test_db):
    """Test that DELETED worlds are excluded from get_worlds_to_scrape."""
    # Insert worlds with different statuses
    await test_db.upsert_world("wrld_pending_1", {"name": "Pending 1"}, "PENDING")
    await test_db.upsert_world("wrld_deleted_1", {"name": "Deleted 1"}, "DELETED")
    await test_db.upsert_world("wrld_deleted_2", {"name": "Deleted 2"}, "DELETED")

    world_ids = await test_db.get_worlds_to_scrape(limit=10)

    # Should only return PENDING world, not DELETED ones
    assert len(world_ids) == 1
    assert "wrld_pending_1" in world_ids
    assert "wrld_deleted_1" not in world_ids
    assert "wrld_deleted_2" not in world_ids


@pytest.mark.asyncio
async def test_marking_world_as_deleted(test_db):
    """Test that marking a world as DELETED actually updates the status."""
    # Create a PENDING world
    await test_db.upsert_world("wrld_test", {"name": "Test World"}, "PENDING")

    # Verify it shows up in the count
    counts_before = await test_db.count_worlds_due_for_scraping()
    assert counts_before["pending"] == 1
    assert counts_before["deleted"] == 0

    # Mark it as DELETED (simulating 404 response)
    await test_db.update_world_status("wrld_test", "DELETED")

    # Verify the status changed
    counts_after = await test_db.count_worlds_due_for_scraping()
    assert counts_after["pending"] == 0
    assert counts_after["deleted"] == 1

    # Verify it doesn't show up in get_worlds_to_scrape
    world_ids = await test_db.get_worlds_to_scrape(limit=10)
    assert "wrld_test" not in world_ids


@pytest.mark.asyncio
async def test_success_world_marked_as_deleted(test_db):
    """Test that SUCCESS worlds can be marked as DELETED when they 404."""
    # Create a SUCCESS world with metadata
    await test_db.upsert_world_with_files(
        "wrld_test",
        {"name": "Test World", "description": "A test world"},
        {
            "heat": 10,
            "popularity": 10,
            "occupants": 2,
            "private_occupants": 0,
            "public_occupants": 2,
            "favorites": 50,
            "visits": 1000,
        },
        [],
        "SUCCESS",
    )

    # Verify it's marked as SUCCESS
    counts_before = await test_db.count_worlds_due_for_scraping()
    assert counts_before["success"] == 1
    assert counts_before["deleted"] == 0

    # Mark it as DELETED (simulating 404 response)
    await test_db.update_world_status("wrld_test", "DELETED")

    # Verify the status changed
    counts_after = await test_db.count_worlds_due_for_scraping()
    assert counts_after["success"] == 0
    assert counts_after["deleted"] == 1

    # Verify it doesn't show up in rescrape queue
    world_ids = await test_db.get_worlds_to_scrape(limit=10)
    assert "wrld_test" not in world_ids


@pytest.mark.asyncio
async def test_recently_scraped_world_can_be_marked_deleted(test_db):
    """Test that even recently-scraped SUCCESS worlds can be marked as DELETED.

    This is a regression test for the bug where upsert_world with empty metadata
    would not update the status if the world had been recently scraped.
    """
    # Create a SUCCESS world with metadata scraped just now
    await test_db.upsert_world_with_files(
        "wrld_recent",
        {"name": "Recently Scraped World", "description": "Just scraped"},
        {
            "heat": 100,
            "popularity": 100,
            "occupants": 50,
            "private_occupants": 25,
            "public_occupants": 25,
            "favorites": 1000,
            "visits": 10000,
        },
        [],
        "SUCCESS",
    )

    # Verify it's marked as SUCCESS
    counts_before = await test_db.count_worlds_due_for_scraping()
    assert counts_before["success"] == 1
    assert counts_before["deleted"] == 0

    # Mark it as DELETED (simulating 404 response during rescrape)
    # This should work even though the world was just scraped
    await test_db.update_world_status("wrld_recent", "DELETED")

    # Verify the status changed to DELETED
    counts_after = await test_db.count_worlds_due_for_scraping()
    assert counts_after["success"] == 0
    assert counts_after["deleted"] == 1

    # Verify it doesn't show up in scrape queue
    world_ids = await test_db.get_worlds_to_scrape(limit=10)
    assert "wrld_recent" not in world_ids
