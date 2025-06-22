"""Fake implementations for testing VRChat scraper components."""

import asyncio
import heapq
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import httpx

from src.vrchat_scraper.models import WorldDetail, WorldSummary


class FakeVRChatAPIClient:
    """Fake VRChat API client for testing."""

    def __init__(self, time_source: Callable[[], float]):
        """Initialize with configurable behavior."""
        self.time_source = time_source
        self.request_log: List[Dict[str, Any]] = []

        # Future-based responses for precise test control
        self.recent_worlds_futures: List[asyncio.Future] = []
        self.world_detail_futures: Dict[str, asyncio.Future] = {}

        # Rate limiting simulation
        self.rate_limit_bucket = 100.0
        self.rate_limit_refill_rate = 10.0  # requests per second
        self.last_refill_time = 0.0

    def add_recent_worlds_future(self, future: asyncio.Future):
        """Add a future for the next get_recent_worlds call."""
        self.recent_worlds_futures.append(future)

    def add_world_detail_future(self, world_id: str, future: asyncio.Future):
        """Add a future for a specific world ID."""
        self.world_detail_futures[world_id] = future

    def set_recent_worlds_response(self, worlds: List[WorldSummary]):
        """Convenience method to set an immediate response."""
        future = asyncio.Future()
        future.set_result(worlds)
        self.add_recent_worlds_future(future)

    def set_world_detail_response(self, world_id: str, world: WorldDetail):
        """Convenience method to set an immediate response."""
        future = asyncio.Future()
        future.set_result(world)
        self.add_world_detail_future(world_id, future)

    def set_recent_worlds_error(self, error: Exception):
        """Convenience method to set an immediate error."""
        future = asyncio.Future()
        future.set_exception(error)
        self.add_recent_worlds_future(future)

    def set_world_detail_error(self, world_id: str, error: Exception):
        """Convenience method to set an immediate error."""
        future = asyncio.Future()
        future.set_exception(error)
        self.add_world_detail_future(world_id, future)

    def simulate_rate_limit(self, enabled: bool = True, rate: float = 10.0):
        """Enable/disable rate limit simulation."""
        if enabled:
            self.rate_limit_refill_rate = rate
            self.rate_limit_bucket = rate * 10  # 10 second bucket
        else:
            self.rate_limit_bucket = float("inf")

    def _refill_bucket(self, now: float):
        """Refill the rate limit bucket."""
        if self.last_refill_time > 0:
            elapsed = now - self.last_refill_time
            refill = elapsed * self.rate_limit_refill_rate
            self.rate_limit_bucket = min(
                self.rate_limit_refill_rate * 10,  # Max bucket size
                self.rate_limit_bucket + refill,
            )
        self.last_refill_time = now

    def _check_rate_limit(self, now: float):
        """Check if request should be rate limited."""
        self._refill_bucket(now)
        if self.rate_limit_bucket >= 1.0:
            self.rate_limit_bucket -= 1.0
            return False  # Not rate limited
        return True  # Rate limited

    async def get_recent_worlds(self) -> List[WorldSummary]:
        """Fake implementation of get_recent_worlds."""
        now = self.time_source()

        self.request_log.append(
            {"endpoint": "recent_worlds", "timestamp": now, "args": [], "kwargs": {}}
        )

        # Check rate limiting before waiting on future
        if self._check_rate_limit(now):
            raise httpx.HTTPStatusError(
                "Rate limited", request=None, response=httpx.Response(429)
            )

        # Get and await the next future
        if self.recent_worlds_futures:
            future = self.recent_worlds_futures.pop(0)
            return await future
        else:
            # No future configured, return empty list
            return []

    async def get_world_details(self, world_id: str) -> WorldDetail:
        """Fake implementation of get_world_details."""
        now = self.time_source()

        self.request_log.append(
            {
                "endpoint": "world_details",
                "timestamp": now,
                "args": [world_id],
                "kwargs": {},
            }
        )

        # Check rate limiting before waiting on future
        if self._check_rate_limit(now):
            raise httpx.HTTPStatusError(
                "Rate limited", request=None, response=httpx.Response(429)
            )

        # Get and await the future for this world
        if world_id in self.world_detail_futures:
            future = self.world_detail_futures.pop(world_id)
            return await future
        else:
            # No future configured, return 404
            raise httpx.HTTPStatusError(
                "World not found", request=None, response=httpx.Response(404)
            )

    def get_request_count(self, endpoint: Optional[str] = None) -> int:
        """Get the number of requests made to an endpoint."""
        if endpoint is None:
            return len(self.request_log)
        return len([r for r in self.request_log if r["endpoint"] == endpoint])

    def clear_request_log(self):
        """Clear the request log."""
        self.request_log.clear()

    def clear_futures(self):
        """Clear all pending futures."""
        self.recent_worlds_futures.clear()
        self.world_detail_futures.clear()

    def set_timeout_error(
        self, world_id: Optional[str] = None, timeout_type: str = "read"
    ):
        """Set a timeout error for recent worlds or specific world."""
        if timeout_type == "read":
            error = httpx.ReadTimeout("Request timed out")
        elif timeout_type == "connect":
            error = httpx.ConnectTimeout("Connection timed out")
        elif timeout_type == "pool":
            error = httpx.PoolTimeout("Pool timeout")
        else:
            error = httpx.TimeoutException("Generic timeout")

        if world_id is None:
            self.set_recent_worlds_error(error)
        else:
            self.set_world_detail_error(world_id, error)


class FakeImageDownloader:
    """Fake image downloader for testing."""

    def __init__(self, time_source: Callable[[], float]):
        """Initialize fake image downloader."""
        self.time_source = time_source
        self.download_log: List[Dict[str, Any]] = []
        self.download_futures: Dict[str, asyncio.Future] = {}
        self.existing_images: Set[str] = set()

    def add_download_future(self, world_id: str, future: asyncio.Future):
        """Add a future for downloading a world's image."""
        self.download_futures[world_id] = future

    def set_download_response(self, world_id: str, success: bool):
        """Convenience method to set an immediate response."""
        future = asyncio.Future()
        future.set_result(success)
        self.add_download_future(world_id, future)

    def set_error_response(self, world_id: str, error: Exception):
        """Convenience method to set an immediate error."""
        future = asyncio.Future()
        future.set_exception(error)
        self.add_download_future(world_id, future)

    def set_image_exists(self, world_id: str, exists: bool = True):
        """Set whether an image exists for a world."""
        if exists:
            self.existing_images.add(world_id)
        else:
            self.existing_images.discard(world_id)

    async def download_image(self, url: str, world_id: str) -> bool:
        """Fake implementation of download_image."""
        now = self.time_source()

        self.download_log.append({"url": url, "world_id": world_id, "timestamp": now})

        # Get and await the future for this download
        if world_id in self.download_futures:
            future = self.download_futures.pop(world_id)
            success = await future
            if success:
                self.existing_images.add(world_id)
            return success
        else:
            # No future configured, default to success
            self.existing_images.add(world_id)
            return True

    def image_exists(self, world_id: str) -> bool:
        """Check if image exists for a world."""
        return world_id in self.existing_images

    def get_download_count(self, world_id: Optional[str] = None) -> int:
        """Get the number of download requests."""
        if world_id is None:
            return len(self.download_log)
        return len([d for d in self.download_log if d["world_id"] == world_id])

    def clear_download_log(self):
        """Clear the download log."""
        self.download_log.clear()

    def clear_futures(self):
        """Clear all pending futures."""
        self.download_futures.clear()


class MockTime:
    """Mock time source for testing."""

    def __init__(self, start_time: float = 1000.0):
        """Initialize with starting time."""
        self._time = start_time
        self._sleep_manager: Optional["MockAsyncSleep"] = None

    def now(self) -> float:
        """Get current time."""
        return self._time

    def set_sleep_manager(self, sleep_manager: "MockAsyncSleep"):
        """Set the sleep manager for coordinated time advancement."""
        self._sleep_manager = sleep_manager

    def advance_sync(self, seconds: float):
        """Synchronously advance time by specified seconds (for simple cases)."""
        self._time += seconds

    async def advance(self, seconds: float):
        """Advance time by specified seconds, processing sleep futures along the way."""
        target_time = self._time + seconds

        # Yield control initially to let coroutines start
        await asyncio.sleep(0)

        while self._time < target_time:
            if not self._sleep_manager:
                # No sleep manager, just advance to target
                self._time = target_time
                break

            # Check for the next sleep that should complete
            next_sleep_time = self._sleep_manager.get_next_sleep_time()

            if next_sleep_time is None or next_sleep_time > target_time:
                # No pending sleeps before target time, advance to target
                self._time = target_time
                break
            else:
                # Advance to the next sleep completion time
                self._time = next_sleep_time
                self._sleep_manager.resolve_sleeps_due_by(self._time)

                # Yield control to let resolved coroutines run
                await asyncio.sleep(0)

    def set_time(self, time: float):
        """Set absolute time."""
        self._time = time


class MockAsyncSleep:
    """Mock async sleep for testing."""

    def __init__(self, mock_time: MockTime):
        """Initialize with mock time source."""
        self.mock_time = mock_time
        self.sleep_log: List[Dict[str, Any]] = []

        # Priority queue of (completion_time, future) pairs
        self._pending_sleeps: List[Tuple[float, asyncio.Future]] = []
        self.auto_advance = True

        # Register with MockTime for coordinated advancement
        mock_time.set_sleep_manager(self)

    async def sleep(self, seconds: float):
        """Mock sleep implementation."""
        start_time = self.mock_time.now()
        end_time = start_time + seconds

        sleep_entry = {
            "duration": seconds,
            "start_time": start_time,
            "end_time": end_time,
        }
        self.sleep_log.append(sleep_entry)

        if self.auto_advance:
            self.mock_time.advance_sync(seconds)
            # Return immediately if auto-advancing
            return
        else:
            # Create a future and add to priority queue
            future = asyncio.Future()
            heapq.heappush(self._pending_sleeps, (end_time, future))
            await future

    def get_next_sleep_time(self) -> Optional[float]:
        """Get the completion time of the next sleep to complete, or None if no pending sleeps."""
        if self._pending_sleeps:
            return self._pending_sleeps[0][0]
        return None

    def resolve_sleeps_due_by(self, time: float):
        """Resolve all sleep futures that should complete by the given time."""
        while self._pending_sleeps and self._pending_sleeps[0][0] <= time:
            _, future = heapq.heappop(self._pending_sleeps)
            if not future.done():
                future.set_result(None)

    def get_total_sleep_time(self) -> float:
        """Get total time spent sleeping."""
        return sum(s["duration"] for s in self.sleep_log)

    def get_sleep_count(self) -> int:
        """Get number of sleep calls."""
        return len(self.sleep_log)

    def clear_sleep_log(self):
        """Clear the sleep log."""
        self.sleep_log.clear()

    def clear_futures(self):
        """Clear all pending sleep futures."""
        self._pending_sleeps.clear()

    def disable_auto_advance(self):
        """Disable automatic time advancement on sleep."""
        self.auto_advance = False

    def enable_auto_advance(self):
        """Enable automatic time advancement on sleep."""
        self.auto_advance = True

    def resolve_next_sleep(self):
        """Resolve the next pending sleep future (for compatibility)."""
        if self._pending_sleeps:
            _, future = heapq.heappop(self._pending_sleeps)
            if not future.done():
                future.set_result(None)

    def resolve_all_sleeps(self):
        """Resolve all pending sleep futures (for compatibility)."""
        while self._pending_sleeps:
            self.resolve_next_sleep()


def create_test_world_summary(
    world_id: str = "wrld_test_123",
    name: str = "Test World",
    author_id: str = "usr_test",
    updated_at: Optional[datetime] = None,
) -> WorldSummary:
    """Create a test WorldSummary object."""
    if updated_at is None:
        updated_at = datetime(2024, 1, 1, 12, 0, 0)  # Fixed time for tests

    return WorldSummary(
        id=world_id,
        name=name,
        authorId=author_id,
        authorName="Test Author",
        imageUrl=f"https://api.vrchat.cloud/api/1/file/file_{world_id}/1/file",
        thumbnailImageUrl=f"https://api.vrchat.cloud/api/1/file/file_{world_id}/1/thumb",
        updated_at=updated_at,
    )


def create_test_world_detail(
    world_id: str = "wrld_test_123",
    name: str = "Test World",
    author_id: str = "usr_test",
    favorites: int = 100,
    visits: int = 1000,
    created_at: Optional[datetime] = None,
    updated_at: Optional[datetime] = None,
) -> WorldDetail:
    """Create a test WorldDetail object."""
    if created_at is None:
        created_at = datetime(2024, 1, 1, 10, 0, 0)  # Fixed time for tests
    if updated_at is None:
        updated_at = datetime(2024, 1, 1, 12, 0, 0)  # Fixed time for tests

    return WorldDetail(
        id=world_id,
        name=name,
        description="A test world for unit testing",
        authorId=author_id,
        imageUrl=f"https://api.vrchat.cloud/api/1/file/file_{world_id}/1/file",
        capacity=32,
        favorites=favorites,
        heat=3,
        popularity=5,
        occupants=12,
        privateOccupants=3,
        publicOccupants=9,
        visits=visits,
        created_at=created_at,
        updated_at=updated_at,
        tags=["system_approved"],
    )


# Note: Use real in-memory SQLite database for unit tests instead of fake database
# This exercises the actual SQLAlchemy code while remaining fast and isolated
