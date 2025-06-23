"""Unit tests for VRChat scraper core functionality."""

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
    create_test_world_summary,
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
def api_rate_limiter(mock_time):
    """Create a rate limiter for API requests."""
    return BBRRateLimiter(mock_time.now(), initial_rate=10.0, name="test_api")


@pytest.fixture
def image_rate_limiter(mock_time):
    """Create a rate limiter for image requests."""
    return BBRRateLimiter(mock_time.now(), initial_rate=20.0, name="test_image")


@pytest.fixture
def stub_api_rate_limiter():
    """Create a stub rate limiter for API requests that never blocks."""
    return StubRateLimiter()


@pytest.fixture
def stub_image_rate_limiter():
    """Create a stub rate limiter for image requests that never blocks."""
    return StubRateLimiter()


@pytest.fixture
def api_circuit_breaker():
    """Create a circuit breaker for API requests."""
    return CircuitBreaker(name="test_api")


@pytest.fixture
def image_circuit_breaker():
    """Create a circuit breaker for image requests."""
    return CircuitBreaker(name="test_image")


@pytest.fixture
def stub_api_circuit_breaker():
    """Create a stub circuit breaker for API requests that never blocks."""
    return StubCircuitBreaker()


@pytest.fixture
def stub_image_circuit_breaker():
    """Create a stub circuit breaker for image requests that never blocks."""
    return StubCircuitBreaker()


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
    stub_scraper,
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

    # Set up successful API response with unity packages for file metadata testing
    test_world = create_test_world_detail(
        world_id=world_id,
        name="Test World",
        favorites=150,
        visits=2000,
        include_unity_packages=True,
    )
    fake_api_client.set_world_detail_response(world_id, test_world)

    # Set up file metadata responses for discovered files
    from tests.fakes import create_test_file_metadata

    discovered_files = test_world.discovered_files
    for file_ref in discovered_files:
        if file_ref.file_type.value == "IMAGE":
            file_metadata = create_test_file_metadata(
                file_id=file_ref.file_id,
                name="test_image.png",
                extension=".png",
                mime_type="image/png",
                version=file_ref.version_number,
            )
        else:  # UNITY_PACKAGE
            file_metadata = create_test_file_metadata(
                file_id=file_ref.file_id,
                name="test_unity.vrcw",
                extension=".vrcw",
                mime_type="application/gzip",
                version=file_ref.version_number,
                file_size=20000000,  # 20MB unity package
            )
        fake_api_client.set_file_metadata_response(file_ref.file_id, file_metadata)

    # Set up successful image downloads for all image files
    for file_ref in discovered_files:
        if file_ref.file_type.value == "IMAGE":
            fake_image_downloader.set_download_result(
                file_ref.file_id, True, f"/fake/path/{file_ref.file_id}.png", 100000, ""
            )

    # Act: Scrape the world and process the full workflow
    await stub_scraper._scrape_world_task(world_id)

    # Process pending file metadata batch
    await stub_scraper._process_pending_file_metadata_batch(limit=100)

    # Process pending image downloads batch
    await stub_scraper._process_pending_image_downloads_batch(limit=100)

    # Assert: Verify the world was updated in database
    worlds_in_db = await test_database.get_worlds_to_scrape(limit=100)
    assert world_id not in worlds_in_db, "World should no longer be pending"

    # Verify API client was called for world details
    assert fake_api_client.get_request_count("world_details") == 1
    world_request = fake_api_client.request_log[0]
    assert world_request["args"] == [world_id]

    # Verify file metadata API calls were made for discovered files
    expected_file_metadata_calls = len([f for f in discovered_files])
    assert (
        fake_api_client.get_request_count("file_metadata")
        == expected_file_metadata_calls
    )

    # Verify file metadata was stored in database
    async with test_database.async_session() as session:
        from src.vrchat_scraper.database import FileMetadata
        from sqlalchemy import select

        file_records = await session.execute(
            select(FileMetadata).where(FileMetadata.world_id == world_id)
        )
        file_records = file_records.scalars().all()
        assert len(file_records) == len(discovered_files)

        # Check that all files have SUCCESS status (metadata was scraped)
        for file_record in file_records:
            assert file_record.scrape_status == "SUCCESS"
            assert file_record.file_metadata is not None

    # Verify image downloads were attempted for image files only
    image_files = [f for f in discovered_files if f.file_type.value == "IMAGE"]
    assert len(fake_image_downloader.download_log) == len(image_files)

    # Verify image download database records
    async with test_database.async_session() as session:
        from src.vrchat_scraper.database import WorldImage

        image_records = await session.execute(select(WorldImage))
        image_records = image_records.scalars().all()
        assert len(image_records) == len(image_files)

        for image_record in image_records:
            assert image_record.download_status == "SUCCESS"
            assert image_record.local_file_path is not None

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


@pytest.fixture
def stub_scraper(
    test_database,
    fake_api_client,
    fake_image_downloader,
    stub_api_rate_limiter,
    stub_image_rate_limiter,
    stub_api_circuit_breaker,
    stub_image_circuit_breaker,
    mock_time,
    mock_sleep,
):
    """Create a VRChat scraper with stub dependencies that don't cause delays."""
    return VRChatScraper(
        database=test_database,
        api_client=fake_api_client,
        image_downloader=fake_image_downloader,
        api_rate_limiter=stub_api_rate_limiter,
        image_rate_limiter=stub_image_rate_limiter,
        api_circuit_breaker=stub_api_circuit_breaker,
        image_circuit_breaker=stub_image_circuit_breaker,
        time_source=mock_time.now,
        sleep_func=mock_sleep.sleep,
    )


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_process_pending_worlds_batch_happy_path(
    stub_scraper,
    test_database,
    fake_api_client,
    fake_image_downloader,
):
    """Test that a batch of pending worlds are processed successfully."""
    # Arrange: Add multiple pending worlds to database
    world_ids = ["wrld_batch_1", "wrld_batch_2", "wrld_batch_3"]

    for world_id in world_ids:
        await test_database.upsert_world(
            world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
        )

        # Set up successful responses for each world
        test_world = create_test_world_detail(
            world_id=world_id,
            name=f"Batch World {world_id[-1]}",
            favorites=100 + int(world_id[-1]),
        )
        fake_api_client.set_world_detail_response(world_id, test_world)
        fake_image_downloader.set_download_response(world_id, True)

    # Act: Process the batch
    processed_count = await stub_scraper._process_pending_worlds_batch(limit=10)

    # Assert: Verify all worlds were processed
    assert processed_count == 3, "Should have processed 3 worlds"

    # Verify no worlds are still pending
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    for world_id in world_ids:
        assert world_id not in pending_worlds, (
            f"World {world_id} should no longer be pending"
        )

    # Verify API calls were made for each world
    assert fake_api_client.get_request_count("world_details") == 3

    # Verify that file metadata entries were created (images will be downloaded later)
    async with test_database.async_session() as session:
        from src.vrchat_scraper.database import FileMetadata
        from sqlalchemy import select

        file_records = await session.execute(select(FileMetadata))
        file_records = file_records.scalars().all()

        # Each world should have one image file entry
        assert len(file_records) == len(world_ids)
        for file_record in file_records:
            assert (
                file_record.scrape_status == "PENDING"
            )  # Ready for file metadata scraping


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_scrape_recent_worlds_batch_happy_path(
    stub_scraper,
    test_database,
    fake_api_client,
):
    """Test that recent worlds discovery batch works correctly."""
    # Arrange: Set up recent worlds response
    world_1 = create_test_world_summary("wrld_recent_1", "Recent World 1")
    world_2 = create_test_world_summary("wrld_recent_2", "Recent World 2")
    recent_worlds = [world_1, world_2]

    fake_api_client.set_recent_worlds_response(recent_worlds)

    # Act: Execute recent worlds discovery batch
    await stub_scraper._scrape_recent_worlds_batch()

    # Assert: Verify worlds were queued as PENDING
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    assert "wrld_recent_1" in pending_worlds, "World 1 should be queued as pending"
    assert "wrld_recent_2" in pending_worlds, "World 2 should be queued as pending"

    # Verify API was called once
    assert fake_api_client.get_request_count("recent_worlds") == 1


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_recent_worlds_batch_handles_authentication_error(
    stub_scraper, fake_api_client
):
    """Test that authentication errors in recent worlds are handled properly."""
    # Arrange: Set up authentication error
    from src.vrchat_scraper.http_client import AuthenticationError

    fake_api_client.set_recent_worlds_error(AuthenticationError("Invalid auth"))

    # Act & Assert: Should raise and shutdown
    with pytest.raises(AuthenticationError):
        await stub_scraper._scrape_recent_worlds_batch()

    # Verify shutdown was called
    assert stub_scraper._shutdown_event.is_set()


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_recent_worlds_batch_handles_http_errors(stub_scraper, fake_api_client):
    """Test that HTTP errors in recent worlds are handled gracefully."""
    # Arrange: Set up HTTP error
    import httpx

    error_response = httpx.Response(429)
    fake_api_client.set_recent_worlds_error(
        httpx.HTTPStatusError("Rate limited", request=None, response=error_response)
    )

    # Act: Should not raise, just handle gracefully
    await stub_scraper._scrape_recent_worlds_batch()

    # Assert: API was called once, error was handled
    assert fake_api_client.get_request_count("recent_worlds") == 1


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_recent_worlds_batch_handles_timeout_errors(
    stub_scraper, fake_api_client
):
    """Test that timeout errors in recent worlds are handled gracefully."""
    # Arrange: Set up timeout error
    import httpx

    fake_api_client.set_recent_worlds_error(httpx.ReadTimeout("Request timed out"))

    # Act: Should not raise, just handle gracefully
    await stub_scraper._scrape_recent_worlds_batch()

    # Assert: API was called once, error was handled
    assert fake_api_client.get_request_count("recent_worlds") == 1


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_scrape_world_handles_authentication_error(
    stub_scraper, test_database, fake_api_client
):
    """Test that authentication errors in world scraping are handled properly."""
    # Arrange
    world_id = "wrld_auth_error_test"
    await test_database.upsert_world(
        world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
    )

    from src.vrchat_scraper.http_client import AuthenticationError

    fake_api_client.set_world_detail_error(
        world_id, AuthenticationError("Invalid auth")
    )

    # Act & Assert: Should raise and shutdown
    with pytest.raises(AuthenticationError):
        await stub_scraper._scrape_world_task(world_id)

    # Verify shutdown was called
    assert stub_scraper._shutdown_event.is_set()


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_scrape_world_handles_404_deleted_world(
    stub_scraper, test_database, fake_api_client
):
    """Test that 404 errors mark world as deleted."""
    # Arrange
    world_id = "wrld_deleted_test"
    await test_database.upsert_world(
        world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
    )

    import httpx

    error_response = httpx.Response(404)
    fake_api_client.set_world_detail_error(
        world_id,
        httpx.HTTPStatusError("Not found", request=None, response=error_response),
    )

    # Act: Should handle gracefully
    await stub_scraper._scrape_world_task(world_id)

    # Assert: World should be marked as DELETED
    async with test_database.async_session() as session:
        from src.vrchat_scraper.database import World
        from sqlalchemy import select

        result = await session.execute(select(World).where(World.world_id == world_id))
        world_record = result.scalar_one()
        assert world_record.scrape_status == "DELETED"


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_scrape_world_handles_other_http_errors(
    stub_scraper, test_database, fake_api_client
):
    """Test that other HTTP errors are handled gracefully."""
    # Arrange
    world_id = "wrld_http_error_test"
    await test_database.upsert_world(
        world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
    )

    import httpx

    error_response = httpx.Response(500)
    fake_api_client.set_world_detail_error(
        world_id,
        httpx.HTTPStatusError("Server error", request=None, response=error_response),
    )

    # Act: Should handle gracefully
    await stub_scraper._scrape_world_task(world_id)

    # Assert: World should remain PENDING (not updated)
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    assert world_id in pending_worlds


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_scrape_world_handles_timeout_errors(
    stub_scraper, test_database, fake_api_client
):
    """Test that timeout errors are handled gracefully."""
    # Arrange
    world_id = "wrld_timeout_test"
    await test_database.upsert_world(
        world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
    )

    import httpx

    fake_api_client.set_world_detail_error(
        world_id, httpx.ConnectTimeout("Connection timed out")
    )

    # Act: Should handle gracefully
    await stub_scraper._scrape_world_task(world_id)

    # Assert: World should remain PENDING
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    assert world_id in pending_worlds


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_image_download_handles_failure(
    stub_scraper, test_database, fake_api_client, fake_image_downloader
):
    """Test that image download failures are handled gracefully."""
    # Arrange
    world_id = "wrld_image_fail_test"
    await test_database.upsert_world(
        world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
    )

    test_world = create_test_world_detail(world_id=world_id, name="Image Fail Test")
    fake_api_client.set_world_detail_response(world_id, test_world)
    fake_image_downloader.set_download_response(world_id, False)  # Failure

    # Act: Should complete world scraping despite image failure
    await stub_scraper._scrape_world_task(world_id)

    # Assert: World should be marked as SUCCESS (image failure doesn't block world scraping)
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    assert world_id not in pending_worlds  # Should be processed successfully


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_scraper_close_method(stub_scraper):
    """Test that the close method works correctly."""
    # Act: Close the scraper
    await stub_scraper.close()

    # Assert: No exceptions should be raised (our fake clients don't have close methods)
    # This test mainly exercises the hasattr checks in the close method


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_image_exists_check(stub_scraper, fake_image_downloader):
    """Test the _image_exists method."""
    # Arrange: Set up an image as existing
    world_id = "wrld_image_exists_test"
    fake_image_downloader.set_image_exists(world_id, True)

    # Act & Assert: Should return True for existing image
    exists = await stub_scraper._image_exists(world_id)
    assert exists is True

    # Act & Assert: Should return False for non-existing image
    exists = await stub_scraper._image_exists("wrld_nonexistent")
    assert exists is False


@pytest.mark.asyncio
@async_timeout(5.0)
async def test_process_pending_worlds_batch_empty_database(stub_scraper):
    """Test that batch processing handles empty database correctly."""
    # Act: Process batch when no worlds are pending
    processed_count = await stub_scraper._process_pending_worlds_batch(limit=10)

    # Assert: Should return 0 for no worlds processed
    assert processed_count == 0
