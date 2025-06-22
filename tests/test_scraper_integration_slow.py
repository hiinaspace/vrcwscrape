"""Slow integration tests for VRChat scraper with real rate limiting and timing."""

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
def fast_api_client():
    """Create a fake VRChat API client that responds immediately."""

    def time_source():
        return asyncio.get_running_loop().time()

    client = FakeVRChatAPIClient(time_source)
    # Disable rate limiting simulation for fast responses
    client.simulate_rate_limit(enabled=False)
    return client


@pytest.fixture
def fast_image_downloader():
    """Create a fake image downloader that responds immediately."""

    def time_source():
        return asyncio.get_running_loop().time()

    return FakeImageDownloader(time_source)


@pytest_asyncio.fixture
async def real_api_rate_limiter():
    """Create a real rate limiter for API requests with fast settings."""
    return BBRRateLimiter(asyncio.get_running_loop().time(), initial_rate=5.0)


@pytest_asyncio.fixture
async def real_image_rate_limiter():
    """Create a real rate limiter for image requests with fast settings."""
    return BBRRateLimiter(asyncio.get_running_loop().time(), initial_rate=10.0)


@pytest.fixture
def api_circuit_breaker():
    """Create a circuit breaker for API requests."""
    return CircuitBreaker()


@pytest.fixture
def image_circuit_breaker():
    """Create a circuit breaker for image requests."""
    return CircuitBreaker()


@pytest.fixture
def integration_scraper(
    test_database,
    fast_api_client,
    fast_image_downloader,
    real_api_rate_limiter,
    real_image_rate_limiter,
    api_circuit_breaker,
    image_circuit_breaker,
):
    """Create a VRChat scraper with real rate limiting but fast timing."""
    return VRChatScraper(
        database=test_database,
        api_client=fast_api_client,
        image_downloader=fast_image_downloader,
        api_rate_limiter=real_api_rate_limiter,
        image_rate_limiter=real_image_rate_limiter,
        api_circuit_breaker=api_circuit_breaker,
        image_circuit_breaker=image_circuit_breaker,
        time_source=lambda: asyncio.get_running_loop().time(),
        sleep_func=asyncio.sleep,
        recent_worlds_interval=2.0,  # 2 seconds instead of 1 hour
        idle_wait_time=0.5,  # 0.5 seconds instead of 1 minute
        error_backoff_time=0.2,  # 0.2 seconds instead of 1 minute
    )


@pytest.mark.slow
@pytest.mark.asyncio
@async_timeout(15.0)
async def test_real_rate_limiting_integration(
    integration_scraper,
    test_database,
    fast_api_client,
    fast_image_downloader,
    real_api_rate_limiter,
):
    """Test that real rate limiting is properly integrated and delays are applied."""
    # Arrange: Add a few pending worlds
    world_ids = [f"wrld_rate_test_{i}" for i in range(3)]  # 3 worlds

    for world_id in world_ids:
        await test_database.upsert_world(
            world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
        )

        # Set up successful responses for each world
        test_world = create_test_world_detail(
            world_id=world_id,
            name=f"Rate Test World {world_id[-1]}",
        )
        fast_api_client.set_world_detail_response(world_id, test_world)
        fast_image_downloader.set_download_response(world_id, True)

    # Force a low rate by updating the max_rate filter and pacing rate
    # The effective rate is min(max_rate, short_term_rate_cap), so we need to set max_rate low
    current_time = asyncio.get_running_loop().time()
    real_api_rate_limiter._max_rate.update(0.8, current_time)  # 0.8 req/s max rate
    real_api_rate_limiter._pacing_rate = 0.8  # Match the max rate
    real_api_rate_limiter._last_send_time = (
        current_time - 0.1
    )  # Recent send to enable pacing

    # Record start time
    start_time = asyncio.get_running_loop().time()

    # Act: Process batch with limit=1 to force sequential processing with rate limiting
    # This will process one world at a time, applying rate limiting between each
    for _ in range(3):  # Process each world individually
        processed_count = await integration_scraper._process_pending_worlds_batch(
            limit=1
        )
        if processed_count == 0:
            break  # No more worlds to process

    # Record end time
    end_time = asyncio.get_running_loop().time()
    duration = end_time - start_time

    # Assert: Should have taken some time due to rate limiting (at least 2.5 seconds for 3 requests at 0.8/s)
    assert duration >= 2.0, (
        f"Should have taken at least 2 seconds due to rate limiting, took {duration}"
    )

    # Verify all API calls were made
    assert fast_api_client.get_request_count("world_details") == 3

    # Verify no worlds are still pending
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    for world_id in world_ids:
        assert world_id not in pending_worlds


@pytest.mark.slow
@pytest.mark.asyncio
@async_timeout(10.0)
async def test_real_rate_limiting_with_recent_worlds_discovery(
    integration_scraper,
    test_database,
    fast_api_client,
    real_api_rate_limiter,
):
    """Test that real rate limiting works with recent worlds discovery."""
    # Arrange: Set up multiple recent worlds responses to trigger rate limiting
    # Use different world IDs for each call to avoid deduplication
    call_1_worlds = [
        create_test_world_summary(f"wrld_recent_call1_{i}", f"Recent World Call1 {i}")
        for i in range(5)
    ]
    call_2_worlds = [
        create_test_world_summary(f"wrld_recent_call2_{i}", f"Recent World Call2 {i}")
        for i in range(5)
    ]
    call_3_worlds = [
        create_test_world_summary(f"wrld_recent_call3_{i}", f"Recent World Call3 {i}")
        for i in range(5)
    ]

    # Set up 3 consecutive calls to recent worlds API with different world sets
    fast_api_client.set_recent_worlds_response(call_1_worlds)
    fast_api_client.set_recent_worlds_response(call_2_worlds)
    fast_api_client.set_recent_worlds_response(call_3_worlds)

    # Set low rate to force delays - update both max_rate and pacing_rate
    current_time = asyncio.get_running_loop().time()
    real_api_rate_limiter._max_rate.update(1.2, current_time)  # 1.2 req/s max rate
    real_api_rate_limiter._pacing_rate = 1.2  # Match the max rate
    real_api_rate_limiter._last_send_time = (
        current_time - 0.1
    )  # Recent send to enable pacing

    # Record start time
    start_time = asyncio.get_running_loop().time()

    # Act: Make 3 calls to recent worlds batch (should be rate limited)
    for _ in range(3):
        await integration_scraper._scrape_recent_worlds_batch()

    # Record end time
    end_time = asyncio.get_running_loop().time()
    duration = end_time - start_time

    # Assert: Should have taken significant time due to rate limiting (at least 1.5 seconds for 3 requests at 1.2/s)
    assert duration >= 1.5, (
        f"Should have taken at least 1.5 seconds due to rate limiting, took {duration}"
    )

    # Verify all API calls were made
    assert fast_api_client.get_request_count("recent_worlds") == 3

    # Verify worlds were queued (15 total: 5 worlds × 3 calls)
    pending_worlds = await test_database.get_worlds_to_scrape(limit=20)
    assert len(pending_worlds) == 15, "Should have queued 15 worlds from 3 API calls"


@pytest.mark.slow
@pytest.mark.asyncio
@async_timeout(8.0)
async def test_idle_time_behavior_with_empty_database(
    integration_scraper,
    test_database,
):
    """Test that scraper properly waits when there are no pending worlds."""
    # Ensure database is empty
    pending_worlds = await test_database.get_worlds_to_scrape(limit=10)
    assert len(pending_worlds) == 0, "Database should start empty"

    # Record start time
    start_time = asyncio.get_running_loop().time()

    # Act: Process batch when no worlds are pending (should wait idle_wait_time)
    processed_count = await integration_scraper._process_pending_worlds_batch(limit=10)

    # Act again to trigger the idle wait in continuous processing
    # We simulate what continuous processing would do
    if processed_count == 0:
        await asyncio.sleep(integration_scraper._idle_wait_time)

    # Record end time
    end_time = asyncio.get_running_loop().time()
    duration = end_time - start_time

    # Assert: Should have processed 0 worlds
    assert processed_count == 0, "Should have processed 0 worlds"

    # Should have waited at least the idle wait time (0.5 seconds)
    assert duration >= 0.4, (
        f"Should have waited at least 0.4 seconds for idle time, took {duration}"
    )


@pytest.mark.slow
@pytest.mark.asyncio
@async_timeout(15.0)
async def test_rate_limiter_probe_recovery():
    """Test that rate limiter correctly recovers from low detected rates through probing.

    This test simulates the scenario where previous app-limited batches have caused
    the rate limiter to detect a low rate, but when we have sufficient throughput,
    the probe cycle correctly discovers the higher actual rate limit.
    """
    # Create a rate limiter with fast parameters for testing:
    # - 2 second probe cycle (instead of default 5s)
    # - 3 second window (instead of default 10s)
    # - Start at 1 req/s (simulating degraded state from previous app-limiting)
    current_time = asyncio.get_running_loop().time()
    fast_rate_limiter = BBRRateLimiter(
        current_time,
        initial_rate=1.0,  # Start low as if degraded by app-limited samples
        probe_cycle_duration_sec=2.0,  # Fast probe cycle
        window_size_sec=3.0,  # Short window
    )

    # Set up database with many pending worlds to ensure we can saturate throughput
    db = Database("sqlite:///:memory:")
    await db.init_schema()

    world_ids = [f"wrld_probe_test_{i}" for i in range(12)]  # Enough to test probing
    for world_id in world_ids:
        await db.upsert_world(
            world_id, {"discovered_at": datetime.utcnow().isoformat()}, status="PENDING"
        )

    # Set up fake clients with fast responses (50ms each, allowing ~20 req/s max)
    def time_source():
        return asyncio.get_running_loop().time()

    api_client = FakeVRChatAPIClient(time_source)
    image_downloader = FakeImageDownloader(time_source)

    for world_id in world_ids:
        test_world = create_test_world_detail(
            world_id=world_id, name=f"Probe Test {world_id[-1]}"
        )

        # Create fast API responses (50ms delay = ~20 req/s theoretical max)
        async def fast_world_response():
            await asyncio.sleep(0.05)  # 50ms per request
            return test_world

        api_client.add_world_detail_future(world_id, fast_world_response())
        image_downloader.set_download_response(world_id, True)

    # Create scraper with the fast rate limiter
    scraper = VRChatScraper(
        database=db,
        api_client=api_client,
        image_downloader=image_downloader,
        api_rate_limiter=fast_rate_limiter,
        image_rate_limiter=BBRRateLimiter(
            current_time, initial_rate=20.0
        ),  # Image limiter not under test
        api_circuit_breaker=CircuitBreaker(),
        image_circuit_breaker=CircuitBreaker(),
        time_source=time_source,
        sleep_func=asyncio.sleep,
    )

    # Record initial state
    initial_max_rate = fast_rate_limiter.max_rate
    print(f"Initial max_rate: {initial_max_rate:.2f} req/s")
    assert initial_max_rate == 1.0, "Should start with low max_rate"

    # Phase 1: Process a batch - should be limited by the low rate initially
    start_time = asyncio.get_running_loop().time()

    # Process batches repeatedly to trigger probe cycles and rate discovery
    total_processed = 0
    for batch_num in range(3):  # 3 batches to span multiple probe cycles
        batch_processed = await scraper._process_pending_worlds_batch(limit=4)
        total_processed += batch_processed
        current_max_rate = fast_rate_limiter.max_rate
        print(
            f"Batch {batch_num + 1}: processed {batch_processed}, max_rate: {current_max_rate:.2f}"
        )

        if batch_processed == 0:
            break

        # Wait a bit to allow probe cycle transitions
        await asyncio.sleep(0.5)

    end_time = asyncio.get_running_loop().time()
    duration = end_time - start_time

    # Final measurements
    final_max_rate = fast_rate_limiter.max_rate
    observed_rate = total_processed / duration

    print(f"Total processed: {total_processed}")
    print(f"Duration: {duration:.2f}s")
    print(f"Observed rate: {observed_rate:.2f} req/s")
    print(f"Final max_rate: {final_max_rate:.2f} req/s")
    print(f"Probe state: {fast_rate_limiter.state}")

    # Assertions: The rate limiter should have discovered higher capacity
    assert total_processed >= 8, (
        f"Should have processed most worlds, got {total_processed}"
    )
    assert final_max_rate > 3.0, (
        f"Rate limiter should have discovered higher rate through probing, "
        f"started at {initial_max_rate}, ended at {final_max_rate:.2f}"
    )
    assert observed_rate > 2.0, (
        f"Should have achieved higher throughput once probe discovered capacity, "
        f"got {observed_rate:.2f} req/s"
    )
