"""Tests for scraper main loop coordination and timing."""

import asyncio
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
    create_test_world_summary,
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
    sleep_mock = MockAsyncSleep(mock_time)
    # Disable auto-advance so we can control timing manually
    sleep_mock.disable_auto_advance()
    return sleep_mock


@pytest.fixture
def fake_api_client(mock_time):
    """Create a fake VRChat API client."""
    return FakeVRChatAPIClient(mock_time.now)


@pytest.fixture
def fake_image_downloader(mock_time):
    """Create a fake image downloader."""
    return FakeImageDownloader(mock_time.now)


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


@pytest.fixture
def api_rate_limiter():
    """Create a stub rate limiter for API requests that never blocks."""
    return StubRateLimiter()


@pytest.fixture
def image_rate_limiter():
    """Create a stub rate limiter for image requests that never blocks."""
    return StubRateLimiter()


@pytest.fixture
def real_api_rate_limiter(mock_time):
    """Create a real rate limiter for tests that need actual rate limiting."""
    return BBRRateLimiter(mock_time.now(), initial_rate=10.0)


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
@async_timeout(15.0)
async def test_recent_worlds_discovery_and_scraping(
    scraper,
    test_database,
    fake_api_client,
    fake_image_downloader,
    mock_time,
    mock_sleep,
):
    """Test that recent worlds are discovered and then scraped by pending task."""
    # Arrange: Set up recent worlds response
    world_1 = create_test_world_summary("wrld_recent_1", "Recent World 1")
    world_2 = create_test_world_summary("wrld_recent_2", "Recent World 2")
    recent_worlds = [world_1, world_2]

    # Set up responses immediately since we'll call directly
    fake_api_client.set_recent_worlds_response(recent_worlds)

    # Set up detailed responses for each world
    detail_1 = create_test_world_detail(
        "wrld_recent_1", "Recent World 1", favorites=100
    )
    detail_2 = create_test_world_detail(
        "wrld_recent_2", "Recent World 2", favorites=200
    )

    fake_api_client.set_world_detail_response("wrld_recent_1", detail_1)
    fake_api_client.set_world_detail_response("wrld_recent_2", detail_2)

    # Set up image downloads
    fake_image_downloader.set_download_response("wrld_recent_1", True)
    fake_image_downloader.set_download_response("wrld_recent_2", True)

    # Act: First run recent worlds discovery to queue worlds
    await scraper._scrape_recent_worlds_task()

    # Verify worlds were queued as PENDING
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    assert "wrld_recent_1" in pending_worlds
    assert "wrld_recent_2" in pending_worlds

    # Now start the pending worlds processor
    pending_task = asyncio.create_task(scraper._process_pending_worlds_continuously())

    # Give it multiple cycles to process the pending worlds
    for i in range(5):  # Multiple cycles to ensure processing
        await asyncio.sleep(0)
        mock_sleep.resolve_all_sleeps()  # Resolve rate limiting delays
        await asyncio.sleep(0)

        # Check if worlds have been processed
        pending_check = await test_database.get_worlds_to_scrape(limit=10)
        if not pending_check:
            break  # All worlds processed

    # Assert: Verify both worlds were scraped successfully
    pending_worlds_after = await test_database.get_worlds_to_scrape(limit=10)
    assert "wrld_recent_1" not in pending_worlds_after, (
        "World 1 should no longer be pending"
    )
    assert "wrld_recent_2" not in pending_worlds_after, (
        "World 2 should no longer be pending"
    )

    # Verify API calls were made (might be more than 2 due to multiple processing cycles)
    assert fake_api_client.get_request_count("recent_worlds") == 1
    assert fake_api_client.get_request_count("world_details") >= 2

    # Verify image downloads (each world should be downloaded at least once)
    assert fake_image_downloader.get_download_count("wrld_recent_1") >= 1
    assert fake_image_downloader.get_download_count("wrld_recent_2") >= 1

    # Clean up
    scraper.shutdown()
    pending_task.cancel()
    try:
        await pending_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
@async_timeout(10.0)
async def test_periodic_timing_with_manual_sleep_control(
    scraper,
    fake_api_client,
    mock_time,
    mock_sleep,
):
    """Test that periodic tasks respect timing and sleep controls work correctly."""
    # Arrange: Set up responses for multiple cycles
    for i in range(3):
        fake_api_client.set_recent_worlds_response([])  # Empty responses

    call_times = []

    # Override the recent worlds task to track timing
    original_scrape_recent = scraper._scrape_recent_worlds_task

    async def track_timing_scrape_recent():
        call_times.append(mock_time.now())
        return await original_scrape_recent()

    scraper._scrape_recent_worlds_task = track_timing_scrape_recent

    # Act: Start recent worlds task
    task = asyncio.create_task(scraper._scrape_recent_worlds_periodically())

    # Let the first iteration start
    await asyncio.sleep(0)

    # Resolve the first iteration's sleeps (should trigger first call)
    mock_sleep.resolve_all_sleeps()
    await asyncio.sleep(0)

    # Should have made first call at time 1000
    assert len(call_times) == 1
    assert call_times[0] == 1000.0

    # Advance time by 1 hour and resolve sleeps (should trigger second call)
    mock_time.advance(3600)  # 1 hour
    mock_sleep.resolve_all_sleeps()
    await asyncio.sleep(0)

    # Should have made second call at time 4600
    assert len(call_times) == 2
    assert call_times[1] == 4600.0

    # Advance time by another hour and resolve sleeps
    mock_time.advance(3600)  # Another hour
    mock_sleep.resolve_all_sleeps()
    await asyncio.sleep(0)

    # Should have made third call at time 8200
    assert len(call_times) == 3
    assert call_times[2] == 8200.0

    # Verify we have proper sleep call tracking
    assert mock_sleep.get_sleep_count() >= 3, "Should have recorded sleep calls"

    # Clean up
    scraper.shutdown()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.asyncio
@async_timeout(10.0)
async def test_pending_worlds_processing_with_rate_limiting(
    test_database,
    fake_api_client,
    fake_image_downloader,
    mock_time,
    mock_sleep,
    real_api_rate_limiter,
    image_rate_limiter,
    api_circuit_breaker,
    image_circuit_breaker,
):
    """Test that pending worlds are processed with proper rate limiting delays."""
    # Create scraper with real rate limiter for this test
    scraper = VRChatScraper(
        database=test_database,
        api_client=fake_api_client,
        image_downloader=fake_image_downloader,
        api_rate_limiter=real_api_rate_limiter,
        image_rate_limiter=image_rate_limiter,
        api_circuit_breaker=api_circuit_breaker,
        image_circuit_breaker=image_circuit_breaker,
        time_source=mock_time.now,
        sleep_func=mock_sleep.sleep,
    )

    # Arrange: Add worlds to database as PENDING
    world_ids = ["wrld_pending_1", "wrld_pending_2", "wrld_pending_3"]
    for world_id in world_ids:
        await test_database.upsert_world(
            world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
        )

        # Set up responses
        detail = create_test_world_detail(world_id, f"Pending World {world_id[-1]}")
        fake_api_client.set_world_detail_response(world_id, detail)
        fake_image_downloader.set_download_response(world_id, True)

    # Configure rate limiter to require delays (lower rate)
    real_api_rate_limiter._pacing_rate = 2.0  # 2 req/s, so 0.5s between requests

    # Act: Start pending worlds processing
    task = asyncio.create_task(scraper._process_pending_worlds_continuously())
    await asyncio.sleep(0)  # Let task start

    # Process worlds one by one, resolving sleeps between them
    for i, world_id in enumerate(world_ids):
        # Advance time first to satisfy rate limiter
        mock_time.advance(1.0)  # Give enough time for rate limiter

        # Resolve any delay sleeps
        mock_sleep.resolve_all_sleeps()
        await asyncio.sleep(0)

        # Give more processing cycles
        for _ in range(3):
            mock_sleep.resolve_all_sleeps()
            await asyncio.sleep(0)

        # Should have made API call for this world
        assert fake_api_client.get_request_count("world_details") >= i + 1

    # Let final processing complete
    mock_sleep.resolve_all_sleeps()
    await asyncio.sleep(0)

    # Assert: All worlds should be processed
    assert fake_api_client.get_request_count("world_details") == 3
    assert fake_image_downloader.get_download_count() == 3

    # Verify no worlds are still pending
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    for world_id in world_ids:
        assert world_id not in pending_worlds

    # Verify sleep was called for rate limiting
    assert mock_sleep.get_sleep_count() > 0, "Should have slept for rate limiting"

    # Clean up
    scraper.shutdown()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
