"""Unit tests for raw API response storage strategy."""

import pytest
import pytest_asyncio
from unittest.mock import patch

from src.vrchat_scraper.database import Database
from src.vrchat_scraper.scraper import VRChatScraper
from src.vrchat_scraper.models import WorldDetail
from tests.fakes import (
    FakeVRChatAPIClient,
    FakeImageDownloader,
    MockTime,
    MockAsyncSleep,
)


class StubRateLimiter:
    """Stub rate limiter that always allows requests through immediately."""

    def get_delay_until_next_request(self, now: float) -> float:
        return 0.0  # Always allow requests immediately

    def on_request_sent(self, request_id: str, timestamp: float):
        pass  # No-op

    def on_success(self, request_id: str, timestamp: float):
        pass  # No-op

    def on_error(self, request_id: str, timestamp: float):
        pass  # No-op


class StubCircuitBreaker:
    """Stub circuit breaker that never blocks requests."""

    def get_delay_until_proceed(self, now: float) -> float:
        return 0.0  # Always allow requests immediately

    def on_success(self):
        pass  # No-op

    def on_error(self, timestamp: float):
        pass  # No-op


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
def stub_scraper(
    test_database,
    fake_api_client,
    fake_image_downloader,
    mock_time,
    mock_sleep,
):
    """Create a VRChat scraper with stub rate limiters and circuit breakers."""
    return VRChatScraper(
        database=test_database,
        api_client=fake_api_client,
        image_downloader=fake_image_downloader,
        api_rate_limiter=StubRateLimiter(),
        image_rate_limiter=StubRateLimiter(),
        api_circuit_breaker=StubCircuitBreaker(),
        image_circuit_breaker=StubCircuitBreaker(),
        time_source=mock_time.now,
        sleep_func=mock_sleep.sleep,
    )


@pytest.mark.asyncio
async def test_world_scraping_stores_raw_api_response(
    stub_scraper, fake_api_client, test_database
):
    """Test that world scraping stores the raw API response instead of pydantic model dump."""
    world_id = "wrld_test_123"

    # Create a raw API response (what VRChat API actually returns)
    raw_api_response = {
        "id": world_id,
        "name": "Test World",
        "description": "A test world",
        "authorId": "usr_test",
        "imageUrl": "https://api.vrchat.cloud/api/1/file/file_447b6078-e5fb-488c-bff5-432d4631f6cf/2/file",
        "thumbnailImageUrl": "https://api.vrchat.cloud/api/1/image/file_447b6078-e5fb-488c-bff5-432d4631f6cf/2/256",
        "capacity": 32,
        "favorites": 100,
        "heat": 3,
        "popularity": 5,
        "occupants": 12,
        "privateOccupants": 3,
        "publicOccupants": 9,
        "visits": 1000,
        "created_at": "2024-01-01T10:00:00.000Z",
        "updated_at": "2024-01-01T12:00:00.000Z",
        "tags": ["system_approved"],
        "unityPackages": [
            {
                "id": "unp_test_123",
                "platform": "standalonewindows",
                "unityVersion": "2019.4.31f1",
                "assetUrl": "https://api.vrchat.cloud/api/1/file/file_unity_123/1/file",
                "assetVersion": 1,
                "created_at": "2024-01-01T10:00:00.000Z",
            }
        ],
        # Extra API fields that should be preserved
        "organization": "vrchat",
        "releaseStatus": "public",
        "version": 55,
        "labsPublicationDate": "2024-01-01T10:00:00.000Z",
        "publicationDate": "2024-01-01T12:00:00.000Z",
    }

    # Mock the API client to return raw JSON instead of pydantic model
    with patch.object(fake_api_client, "get_world_details") as mock_get_world_details:
        # The API client should return raw JSON, not a parsed WorldDetail object
        mock_get_world_details.return_value = raw_api_response

        # Setup world as pending
        await test_database.upsert_world(world_id, {}, "PENDING")

        # Act: Scrape the world
        await stub_scraper._scrape_world_task(world_id)

        # Assert: Check that the stored metadata is the raw API response
        async with test_database.async_session() as session:
            from src.vrchat_scraper.database import World
            from sqlalchemy import select

            result = await session.execute(
                select(World).where(World.world_id == world_id)
            )
            world = result.scalar_one()

            stored_metadata = world.world_metadata

            # The stored metadata should be exactly the raw API response
            assert stored_metadata == raw_api_response

            # Verify that extra API fields are preserved (these would be lost in current implementation)
            assert stored_metadata["organization"] == "vrchat"
            assert stored_metadata["releaseStatus"] == "public"
            assert stored_metadata["version"] == 55
            assert stored_metadata["labsPublicationDate"] == "2024-01-01T10:00:00.000Z"
            assert stored_metadata["publicationDate"] == "2024-01-01T12:00:00.000Z"

            # Verify that ephemeral fields are included (current implementation removes them)
            assert stored_metadata["favorites"] == 100
            assert stored_metadata["occupants"] == 12
            assert stored_metadata["visits"] == 1000

            # Most importantly: verify that computed fields are NOT included
            # The current implementation includes 'discovered_files' from the pydantic computed property
            assert "discovered_files" not in stored_metadata


@pytest.mark.asyncio
async def test_world_scraping_still_extracts_files_and_metrics(
    stub_scraper, fake_api_client, test_database
):
    """Test that file discovery and metrics extraction still work with raw API storage."""
    world_id = "wrld_test_456"

    raw_api_response = {
        "id": world_id,
        "name": "Test World 2",
        "authorId": "usr_test",
        "imageUrl": "https://api.vrchat.cloud/api/1/file/file_image_456/1/file",
        "thumbnailImageUrl": "https://api.vrchat.cloud/api/1/image/file_image_456/1/256",
        "capacity": 16,
        "favorites": 50,
        "heat": 2,
        "popularity": 3,
        "occupants": 5,
        "privateOccupants": 2,
        "publicOccupants": 3,
        "visits": 500,
        "created_at": "2024-01-01T10:00:00.000Z",
        "updated_at": "2024-01-01T12:00:00.000Z",
        "tags": [],
        "unityPackages": [
            {
                "id": "unp_test_456",
                "platform": "standalonewindows",
                "unityVersion": "2022.3.22f1",
                "assetUrl": "https://api.vrchat.cloud/api/1/file/file_unity_456/2/file",
                "assetVersion": 2,
            }
        ],
    }

    with patch.object(fake_api_client, "get_world_details") as mock_get_world_details:
        mock_get_world_details.return_value = raw_api_response

        await test_database.upsert_world(world_id, {}, "PENDING")

        # Act: Scrape the world
        await stub_scraper._scrape_world_task(world_id)

        # Assert: Verify that files were discovered and queued
        file_metadata_list = await test_database.get_pending_file_metadata(limit=10)

        # Should have discovered 2 files: 1 image + 1 unity package
        assert len(file_metadata_list) == 2

        # Check that files were extracted correctly
        file_ids = [file_id for file_id, file_type in file_metadata_list]
        assert "file_image_456" in file_ids
        assert "file_unity_456" in file_ids

        # Verify metrics were extracted and stored
        async with test_database.async_session() as session:
            from src.vrchat_scraper.database import WorldMetrics
            from sqlalchemy import select

            result = await session.execute(
                select(WorldMetrics).where(WorldMetrics.world_id == world_id)
            )
            metrics = result.scalar_one()

            assert metrics.favorites == 50
            assert metrics.occupants == 5
            assert metrics.visits == 500


@pytest.mark.asyncio
async def test_raw_api_response_preserves_unknown_fields(
    stub_scraper, fake_api_client, test_database
):
    """Test that unknown/future API fields are preserved in raw storage."""
    world_id = "wrld_future_test"

    # Simulate API response with new fields that don't exist in our pydantic model
    raw_api_response = {
        "id": world_id,
        "name": "Future World",
        "authorId": "usr_test",
        "imageUrl": "https://api.vrchat.cloud/api/1/file/file_future/1/file",
        "thumbnailImageUrl": "https://api.vrchat.cloud/api/1/image/file_future/1/256",
        "capacity": 32,
        "favorites": 75,
        "heat": 4,
        "popularity": 6,
        "occupants": 8,
        "privateOccupants": 3,
        "publicOccupants": 5,
        "visits": 2000,
        "created_at": "2024-01-01T10:00:00.000Z",
        "updated_at": "2024-01-01T12:00:00.000Z",
        "tags": ["system_approved"],
        "unityPackages": [],
        # Future fields that don't exist in our current pydantic model
        "newFeatureFlag": True,
        "experimentalSettings": {"enableNewPhysics": True, "maxDrawCalls": 1000},
        "aiGeneratedContent": {"hasAIArt": False, "aiContentTypes": []},
    }

    with patch.object(fake_api_client, "get_world_details") as mock_get_world_details:
        mock_get_world_details.return_value = raw_api_response

        await test_database.upsert_world(world_id, {}, "PENDING")

        # Act: Scrape the world
        await stub_scraper._scrape_world_task(world_id)

        # Assert: Verify that future/unknown fields are preserved
        async with test_database.async_session() as session:
            from src.vrchat_scraper.database import World
            from sqlalchemy import select

            result = await session.execute(
                select(World).where(World.world_id == world_id)
            )
            world = result.scalar_one()

            stored_metadata = world.world_metadata

            # Verify future fields are preserved exactly
            assert stored_metadata["newFeatureFlag"] is True
            assert stored_metadata["experimentalSettings"]["enableNewPhysics"] is True
            assert stored_metadata["experimentalSettings"]["maxDrawCalls"] == 1000
            assert stored_metadata["aiGeneratedContent"]["hasAIArt"] is False
            assert stored_metadata["aiGeneratedContent"]["aiContentTypes"] == []

            # Verify that the entire response structure is preserved
            assert stored_metadata == raw_api_response


@pytest.mark.asyncio
async def test_current_implementation_adds_computed_fields(test_database):
    """Test that demonstrates the current implementation problem - computed fields are added."""
    world_id = "wrld_current_problem"

    # Simulate what the current implementation does
    # This test should PASS with current code and FAIL after our refactor
    world_detail = WorldDetail(
        id=world_id,
        name="Problem World",
        description="Demonstrates the current issue",
        authorId="usr_test",
        imageUrl="https://api.vrchat.cloud/api/1/file/file_problem/1/file",
        thumbnailImageUrl="https://api.vrchat.cloud/api/1/image/file_problem/1/256",
        capacity=32,
        favorites=100,
        heat=3,
        popularity=5,
        occupants=10,
        privateOccupants=4,
        publicOccupants=6,
        visits=1500,
        created_at="2024-01-01T10:00:00.000Z",
        updated_at="2024-01-01T12:00:00.000Z",
        tags=["system_approved"],
        unityPackages=[],
    )

    # Current implementation stores stable_metadata() which includes computed fields
    stable_metadata = world_detail.stable_metadata()

    # The current stable_metadata() includes the computed 'discovered_files' field
    assert "discovered_files" in stable_metadata
    assert len(stable_metadata["discovered_files"]) == 1  # One image file discovered

    # Store using current approach
    await test_database.upsert_world(world_id, stable_metadata, "SUCCESS")

    # Verify the computed field was stored (this is the problem we're fixing)
    async with test_database.async_session() as session:
        from src.vrchat_scraper.database import World
        from sqlalchemy import select

        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()

        stored_metadata = world.world_metadata

        # Current implementation stores the computed field (this is the redundancy we want to eliminate)
        assert "discovered_files" in stored_metadata
        assert len(stored_metadata["discovered_files"]) == 1

        # Also verify that ephemeral fields are missing (current implementation removes them)
        assert "favorites" not in stored_metadata
        assert "occupants" not in stored_metadata
        assert "visits" not in stored_metadata
