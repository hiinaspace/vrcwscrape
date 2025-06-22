"""Integration tests for VRChat scraper."""

import pytest
import pytest_asyncio
from datetime import datetime

from tests.test_utils import async_timeout

from src.vrchat_scraper.circuit_breaker import CircuitBreaker
from src.vrchat_scraper.database import Database
from src.vrchat_scraper.rate_limiter import BBRRateLimiter
from src.vrchat_scraper.scraper import VRChatScraper
from tests.fakes import (
    FakeVRChatAPIClient,
    FakeImageDownloader,
    MockTime,
    MockAsyncSleep,
    create_test_world_detail,
)


@pytest_asyncio.fixture
async def test_database():
    """Create an in-memory SQLite database for testing."""
    db = Database("sqlite:///:memory:")
    await db.init_schema()
    return db


@pytest.fixture
def mock_time():
    """Create a mock time source."""
    return MockTime(start_time=1000.0)


@pytest.fixture
def mock_sleep(mock_time):
    """Create a mock async sleep function."""
    return MockAsyncSleep(mock_time)


@pytest.fixture
def fake_api_client(mock_time):
    """Create a fake VRChat API client."""
    return FakeVRChatAPIClient(mock_time.now)


@pytest.fixture
def fake_image_downloader(mock_time):
    """Create a fake image downloader."""
    return FakeImageDownloader(mock_time.now)


@pytest.fixture
def api_rate_limiter(mock_time):
    """Create a rate limiter for API requests."""
    return BBRRateLimiter(mock_time.now(), initial_rate=10.0)


@pytest.fixture
def image_rate_limiter(mock_time):
    """Create a rate limiter for image requests."""
    return BBRRateLimiter(mock_time.now(), initial_rate=20.0)


@pytest.fixture
def api_circuit_breaker():
    """Create a circuit breaker for API requests."""
    return CircuitBreaker()


@pytest.fixture
def image_circuit_breaker():
    """Create a circuit breaker for image requests."""
    return CircuitBreaker()


@pytest.fixture
def scraper(
    test_database,
    fake_api_client,
    fake_image_downloader,
    api_rate_limiter,
    image_rate_limiter,
    api_circuit_breaker,
    image_circuit_breaker,
    mock_time,
    mock_sleep,
):
    """Create a VRChat scraper with all dependencies."""
    return VRChatScraper(
        database=test_database,
        api_client=fake_api_client,
        image_downloader=fake_image_downloader,
        api_rate_limiter=api_rate_limiter,
        image_rate_limiter=image_rate_limiter,
        api_circuit_breaker=api_circuit_breaker,
        image_circuit_breaker=image_circuit_breaker,
        time_source=mock_time.now,
        sleep_func=mock_sleep.sleep,
    )


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_scrape_world_happy_path(
    scraper,
    test_database,
    fake_api_client,
    fake_image_downloader,
    mock_time,
):
    """Test successful scraping of a single world."""
    # Arrange: Set up a world to scrape in the database
    world_id = "wrld_test_123"
    await test_database.upsert_world(
        world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
    )

    # Set up successful API response
    test_world = create_test_world_detail(
        world_id=world_id,
        name="Test World",
        favorites=150,
        visits=2000,
    )
    fake_api_client.set_world_detail_response(world_id, test_world)

    # Set up successful image download
    fake_image_downloader.set_download_response(world_id, True)

    # Act: Scrape the world
    await scraper._scrape_world_task(world_id)

    # Assert: Verify the world was updated in database
    worlds_in_db = await test_database.get_worlds_to_scrape(limit=100)
    assert world_id not in worlds_in_db, "World should no longer be pending"

    # Verify API client was called
    assert fake_api_client.get_request_count("world_details") == 1
    world_request = fake_api_client.request_log[0]
    assert world_request["args"] == [world_id]

    # Verify image download was attempted
    assert fake_image_downloader.get_download_count(world_id) == 1
    download_request = fake_image_downloader.download_log[0]
    assert download_request["world_id"] == world_id
    assert download_request["url"] == test_world.image_url

    # Verify image exists now
    assert fake_image_downloader.image_exists(world_id)

    # Verify no sleep was needed (no delays from rate limiter/circuit breaker)
    assert mock_time._time == 1000.0, "Time should not have advanced"


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_scrape_world_with_database_state(
    scraper,
    test_database,
    fake_api_client,
    fake_image_downloader,
):
    """Test that world metadata and metrics are correctly stored in database."""
    # Arrange
    world_id = "wrld_database_test"
    await test_database.upsert_world(
        world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
    )

    test_world = create_test_world_detail(
        world_id=world_id,
        name="Database Test World",
        favorites=250,
        visits=5000,
    )
    fake_api_client.set_world_detail_response(world_id, test_world)
    fake_image_downloader.set_download_response(world_id, True)

    # Act
    await scraper._scrape_world_task(world_id)

    # Assert: Check database state directly
    async with test_database.async_session() as session:
        from src.vrchat_scraper.database import World, WorldMetrics
        from sqlalchemy import select

        # Check world metadata
        result = await session.execute(select(World).where(World.world_id == world_id))
        world_record = result.scalar_one()

        assert world_record.world_id == world_id
        assert world_record.scrape_status == "SUCCESS"
        assert world_record.world_metadata["name"] == "Database Test World"
        assert world_record.world_metadata["id"] == world_id
        # Verify ephemeral fields were removed from metadata
        assert "favorites" not in world_record.world_metadata
        assert "visits" not in world_record.world_metadata

        # Check metrics were stored separately
        result = await session.execute(
            select(WorldMetrics).where(WorldMetrics.world_id == world_id)
        )
        metrics_record = result.scalar_one()

        assert metrics_record.world_id == world_id
        assert metrics_record.favorites == 250
        assert metrics_record.visits == 5000
        assert metrics_record.occupants == 12  # From test data
