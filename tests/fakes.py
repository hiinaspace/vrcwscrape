"""Fake implementations for testing VRChat scraper components."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import httpx

from src.vrchat_scraper.models import WorldDetail, WorldSummary


class FakeVRChatAPIClient:
    """Fake VRChat API client for testing."""

    def __init__(self):
        """Initialize with configurable behavior."""
        self.recent_worlds_responses: List[List[WorldSummary]] = []
        self.world_detail_responses: Dict[str, WorldDetail] = {}
        self.error_responses: Dict[str, Exception] = {}
        self.request_log: List[Dict[str, Any]] = []
        self.request_delays: Dict[str, float] = {}

        # Rate limiting simulation
        self.rate_limit_bucket = 100.0
        self.rate_limit_refill_rate = 10.0  # requests per second
        self.last_refill_time = 0.0

    def set_recent_worlds_response(self, worlds: List[WorldSummary]):
        """Set the response for get_recent_worlds."""
        self.recent_worlds_responses = [worlds]

    def set_world_detail_response(self, world_id: str, world: WorldDetail):
        """Set the response for a specific world ID."""
        self.world_detail_responses[world_id] = world

    def set_error_response(self, endpoint: str, error: Exception):
        """Set an error response for an endpoint."""
        self.error_responses[endpoint] = error

    def set_request_delay(self, endpoint: str, delay: float):
        """Set a delay for requests to an endpoint."""
        self.request_delays[endpoint] = delay

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
        endpoint = "recent_worlds"
        now = asyncio.get_event_loop().time()

        self.request_log.append(
            {"endpoint": endpoint, "timestamp": now, "args": [], "kwargs": {}}
        )

        # Simulate delay
        if endpoint in self.request_delays:
            await asyncio.sleep(self.request_delays[endpoint])

        # Check for errors
        if endpoint in self.error_responses:
            raise self.error_responses[endpoint]

        # Check rate limiting
        if self._check_rate_limit(now):
            raise httpx.HTTPStatusError(
                "Rate limited", request=None, response=httpx.Response(429)
            )

        # Return configured response or empty list
        if self.recent_worlds_responses:
            return self.recent_worlds_responses[0]
        return []

    async def get_world_details(self, world_id: str) -> WorldDetail:
        """Fake implementation of get_world_details."""
        endpoint = f"world_details_{world_id}"
        now = asyncio.get_event_loop().time()

        self.request_log.append(
            {
                "endpoint": "world_details",
                "timestamp": now,
                "args": [world_id],
                "kwargs": {},
            }
        )

        # Simulate delay
        if endpoint in self.request_delays or "world_details" in self.request_delays:
            delay = self.request_delays.get(
                endpoint, self.request_delays.get("world_details", 0)
            )
            await asyncio.sleep(delay)

        # Check for specific world errors
        if endpoint in self.error_responses:
            raise self.error_responses[endpoint]

        # Check for general world detail errors
        if "world_details" in self.error_responses:
            raise self.error_responses["world_details"]

        # Check rate limiting
        if self._check_rate_limit(now):
            raise httpx.HTTPStatusError(
                "Rate limited", request=None, response=httpx.Response(429)
            )

        # Return configured response or raise 404
        if world_id in self.world_detail_responses:
            return self.world_detail_responses[world_id]

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

    def set_timeout_error(self, endpoint: str, timeout_type: str = "read"):
        """Set a timeout error for an endpoint."""
        if timeout_type == "read":
            error = httpx.ReadTimeout("Request timed out")
        elif timeout_type == "connect":
            error = httpx.ConnectTimeout("Connection timed out")
        elif timeout_type == "pool":
            error = httpx.PoolTimeout("Pool timeout")
        else:
            error = httpx.TimeoutException("Generic timeout")
        self.set_error_response(endpoint, error)


class FakeImageDownloader:
    """Fake image downloader for testing."""

    def __init__(self):
        """Initialize fake image downloader."""
        self.download_log: List[Dict[str, Any]] = []
        self.success_responses: Dict[str, bool] = {}
        self.error_responses: Dict[str, Exception] = {}
        self.request_delays: Dict[str, float] = {}
        self.existing_images: Set[str] = set()

    def set_download_response(self, world_id: str, success: bool):
        """Set the response for downloading a world's image."""
        self.success_responses[world_id] = success

    def set_error_response(self, world_id: str, error: Exception):
        """Set an error response for downloading a world's image."""
        self.error_responses[world_id] = error

    def set_request_delay(self, world_id: str, delay: float):
        """Set a delay for downloading a world's image."""
        self.request_delays[world_id] = delay

    def set_image_exists(self, world_id: str, exists: bool = True):
        """Set whether an image exists for a world."""
        if exists:
            self.existing_images.add(world_id)
        else:
            self.existing_images.discard(world_id)

    async def download_image(self, url: str, world_id: str) -> bool:
        """Fake implementation of download_image."""
        now = asyncio.get_event_loop().time()

        self.download_log.append({"url": url, "world_id": world_id, "timestamp": now})

        # Simulate delay
        if world_id in self.request_delays:
            await asyncio.sleep(self.request_delays[world_id])

        # Check for errors
        if world_id in self.error_responses:
            raise self.error_responses[world_id]

        # Return configured response or default success
        if world_id in self.success_responses:
            success = self.success_responses[world_id]
            if success:
                self.existing_images.add(world_id)
            return success

        # Default to success
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


class MockTime:
    """Mock time source for testing."""

    def __init__(self, start_time: float = 1000.0):
        """Initialize with starting time."""
        self._time = start_time

    def now(self) -> float:
        """Get current time."""
        return self._time

    def advance(self, seconds: float):
        """Advance time by specified seconds."""
        self._time += seconds

    def set_time(self, time: float):
        """Set absolute time."""
        self._time = time


class MockAsyncSleep:
    """Mock async sleep for testing."""

    def __init__(self, mock_time: MockTime):
        """Initialize with mock time source."""
        self.mock_time = mock_time
        self.sleep_log: List[Dict[str, Any]] = []
        self.auto_advance = True

    async def sleep(self, seconds: float):
        """Mock sleep implementation."""
        start_time = self.mock_time.now()
        self.sleep_log.append(
            {
                "duration": seconds,
                "start_time": start_time,
                "end_time": start_time + seconds,
            }
        )

        if self.auto_advance:
            self.mock_time.advance(seconds)

        # Actually sleep a tiny amount to yield control
        await asyncio.sleep(0.001)

    def get_total_sleep_time(self) -> float:
        """Get total time spent sleeping."""
        return sum(s["duration"] for s in self.sleep_log)

    def get_sleep_count(self) -> int:
        """Get number of sleep calls."""
        return len(self.sleep_log)

    def clear_sleep_log(self):
        """Clear the sleep log."""
        self.sleep_log.clear()

    def disable_auto_advance(self):
        """Disable automatic time advancement on sleep."""
        self.auto_advance = False

    def enable_auto_advance(self):
        """Enable automatic time advancement on sleep."""
        self.auto_advance = True


def create_test_world_summary(
    world_id: str = "wrld_test_123",
    name: str = "Test World",
    author_id: str = "usr_test",
) -> WorldSummary:
    """Create a test WorldSummary object."""
    return WorldSummary(
        id=world_id,
        name=name,
        authorId=author_id,
        authorName="Test Author",
        imageUrl=f"https://api.vrchat.cloud/api/1/file/file_{world_id}/1/file",
        thumbnailImageUrl=f"https://api.vrchat.cloud/api/1/file/file_{world_id}/1/thumb",
        updated_at=datetime.utcnow(),
    )


def create_test_world_detail(
    world_id: str = "wrld_test_123",
    name: str = "Test World",
    author_id: str = "usr_test",
    favorites: int = 100,
    visits: int = 1000,
) -> WorldDetail:
    """Create a test WorldDetail object."""
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
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        tags=["system_approved"],
    )


class FakeDatabase:
    """Fake database for testing."""

    def __init__(self):
        """Initialize fake database."""
        self.worlds: Dict[str, Dict[str, Any]] = {}
        self.metrics: List[Dict[str, Any]] = []
        self.operation_log: List[Dict[str, Any]] = []

    async def init_schema(self):
        """Fake schema initialization."""
        self.operation_log.append({"operation": "init_schema"})

    async def upsert_world(
        self, world_id: str, metadata: dict, status: str = "SUCCESS"
    ):
        """Fake world upsert."""
        self.operation_log.append(
            {
                "operation": "upsert_world",
                "world_id": world_id,
                "metadata": metadata,
                "status": status,
            }
        )

        if world_id in self.worlds:
            self.worlds[world_id].update(
                {
                    "metadata": metadata,
                    "status": status,
                    "last_scrape_time": datetime.utcnow(),
                }
            )
        else:
            self.worlds[world_id] = {
                "world_id": world_id,
                "metadata": metadata,
                "status": status,
                "last_scrape_time": datetime.utcnow(),
            }

    async def insert_metrics(
        self, world_id: str, metrics: Dict[str, int], scrape_time: datetime
    ):
        """Fake metrics insertion."""
        self.operation_log.append(
            {
                "operation": "insert_metrics",
                "world_id": world_id,
                "metrics": metrics,
                "scrape_time": scrape_time,
            }
        )

        self.metrics.append(
            {"world_id": world_id, "scrape_time": scrape_time, **metrics}
        )

    async def get_worlds_to_scrape(self, limit: int = 100) -> List[str]:
        """Fake get worlds to scrape."""
        self.operation_log.append({"operation": "get_worlds_to_scrape", "limit": limit})

        # Return worlds with PENDING status
        pending_worlds = [
            world_id
            for world_id, world_data in self.worlds.items()
            if world_data["status"] == "PENDING"
        ]

        return pending_worlds[:limit]

    def get_world(self, world_id: str) -> Optional[Dict[str, Any]]:
        """Get a world from the fake database."""
        return self.worlds.get(world_id)

    def get_world_metrics(self, world_id: str) -> List[Dict[str, Any]]:
        """Get metrics for a world."""
        return [m for m in self.metrics if m["world_id"] == world_id]

    def clear(self):
        """Clear all data."""
        self.worlds.clear()
        self.metrics.clear()
        self.operation_log.clear()
