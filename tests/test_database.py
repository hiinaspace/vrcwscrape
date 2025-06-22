"""Tests for database operations."""

import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from sqlalchemy import select

from vrchat_scraper.database import Database, World, WorldMetrics, ScrapeStatus


@pytest_asyncio.fixture
async def test_db():
    """Create test database using in-memory SQLite."""
    db = Database("sqlite:///:memory:")
    await db.init_schema()
    return db


@pytest.mark.asyncio
async def test_init_schema(test_db):
    """Test schema initialization creates tables."""
    # Schema already initialized in fixture
    async with test_db.async_session() as session:
        # Test we can query empty tables without error
        result = await session.execute(select(World))
        assert result.fetchall() == []
        
        result = await session.execute(select(WorldMetrics))
        assert result.fetchall() == []


@pytest.mark.asyncio
async def test_upsert_world_insert(test_db):
    """Test inserting a new world."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World", "description": "A test world"}
    
    await test_db.upsert_world(world_id, metadata, "PENDING")
    
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        
        assert world.world_id == world_id
        assert world.world_metadata == metadata
        assert world.scrape_status == ScrapeStatus.PENDING
        assert world.last_scrape_time is not None


@pytest.mark.asyncio
async def test_upsert_world_update(test_db):
    """Test updating an existing world."""
    world_id = "wrld_test_123"
    initial_metadata = {"name": "Test World", "description": "A test world"}
    updated_metadata = {"name": "Updated World", "description": "An updated test world"}
    
    # Insert initial world
    await test_db.upsert_world(world_id, initial_metadata, "PENDING")
    
    # Update the world
    await test_db.upsert_world(world_id, updated_metadata, "SUCCESS")
    
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        
        assert world.world_id == world_id
        assert world.world_metadata == updated_metadata
        assert world.scrape_status == ScrapeStatus.SUCCESS


@pytest.mark.asyncio
async def test_world_state_transitions(test_db):
    """Test world moves through states correctly."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    
    # Start as PENDING
    await test_db.upsert_world(world_id, metadata, "PENDING")
    
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        assert world.scrape_status == ScrapeStatus.PENDING
    
    # Move to SUCCESS
    await test_db.upsert_world(world_id, metadata, "SUCCESS")
    
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        assert world.scrape_status == ScrapeStatus.SUCCESS
    
    # Move to DELETED
    await test_db.upsert_world(world_id, metadata, "DELETED")
    
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        assert world.scrape_status == ScrapeStatus.DELETED


@pytest.mark.asyncio
async def test_insert_metrics(test_db):
    """Test inserting world metrics."""
    world_id = "wrld_test_123"
    scrape_time = datetime.utcnow()
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500
    }
    
    await test_db.insert_metrics(world_id, metrics, scrape_time)
    
    async with test_db.async_session() as session:
        result = await session.execute(
            select(WorldMetrics).where(
                WorldMetrics.world_id == world_id,
                WorldMetrics.scrape_time == scrape_time
            )
        )
        saved_metrics = result.scalar_one()
        
        assert saved_metrics.world_id == world_id
        assert saved_metrics.scrape_time == scrape_time
        assert saved_metrics.favorites == 100
        assert saved_metrics.heat == 5
        assert saved_metrics.popularity == 8
        assert saved_metrics.occupants == 12
        assert saved_metrics.private_occupants == 3
        assert saved_metrics.public_occupants == 9
        assert saved_metrics.visits == 1500


@pytest.mark.asyncio
async def test_metrics_append_only(test_db):
    """Test metrics are appended, not updated."""
    world_id = "wrld_test_123"
    time1 = datetime.utcnow()
    time2 = time1 + timedelta(hours=1)
    
    metrics1 = {
        "favorites": 100, "heat": 5, "popularity": 8, "occupants": 12,
        "private_occupants": 3, "public_occupants": 9, "visits": 1500
    }
    metrics2 = {
        "favorites": 120, "heat": 6, "popularity": 9, "occupants": 15,
        "private_occupants": 4, "public_occupants": 11, "visits": 1600
    }
    
    await test_db.insert_metrics(world_id, metrics1, time1)
    await test_db.insert_metrics(world_id, metrics2, time2)
    
    async with test_db.async_session() as session:
        result = await session.execute(
            select(WorldMetrics).where(WorldMetrics.world_id == world_id)
        )
        all_metrics = result.fetchall()
        
        assert len(all_metrics) == 2
        
        # Verify both records exist with different values
        metrics_by_time = {m[0].scrape_time: m[0] for m in all_metrics}
        assert metrics_by_time[time1].favorites == 100
        assert metrics_by_time[time2].favorites == 120


@pytest.mark.asyncio
async def test_get_worlds_to_scrape_pending(test_db):
    """Test getting pending worlds for scraping."""
    # Insert worlds in different states
    await test_db.upsert_world("wrld_pending_1", {"name": "Pending 1"}, "PENDING")
    await test_db.upsert_world("wrld_pending_2", {"name": "Pending 2"}, "PENDING")
    await test_db.upsert_world("wrld_success_1", {"name": "Success 1"}, "SUCCESS")
    
    world_ids = await test_db.get_worlds_to_scrape(limit=10)
    
    # Should return only pending worlds
    assert len(world_ids) == 2
    assert "wrld_pending_1" in world_ids
    assert "wrld_pending_2" in world_ids
    assert "wrld_success_1" not in world_ids


@pytest.mark.asyncio
async def test_get_worlds_to_scrape_rescrape_logic(test_db):
    """Test rescrape scheduling logic."""
    now = datetime.utcnow()
    old_time = now - timedelta(hours=25)  # Should trigger rescrape (>24h for new worlds)
    
    # Create a world that was scraped 25 hours ago (needs rescrape)
    world_id = "wrld_old_success"
    await test_db.upsert_world(world_id, {"name": "Old Success"}, "SUCCESS")
    
    # Manually update the last_scrape_time to be old
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        world.last_scrape_time = old_time
        world.publish_date = now - timedelta(days=1)  # New world (< 7 days old)
        await session.commit()
    
    world_ids = await test_db.get_worlds_to_scrape(limit=10)
    
    # Should include the old world for rescraping
    assert world_id in world_ids


@pytest.mark.asyncio
async def test_get_worlds_to_scrape_limit(test_db):
    """Test scrape limit is respected."""
    # Insert more worlds than the limit
    for i in range(5):
        await test_db.upsert_world(f"wrld_pending_{i}", {"name": f"Pending {i}"}, "PENDING")
    
    world_ids = await test_db.get_worlds_to_scrape(limit=3)
    
    assert len(world_ids) == 3
