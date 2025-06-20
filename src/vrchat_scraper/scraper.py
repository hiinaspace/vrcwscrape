"""Core scraping logic for VRChat world metadata."""

import logging
from datetime import datetime
from pathlib import Path

import httpx

from .database import Database
from .models import WorldDetail, WorldSummary
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Raised when authentication fails."""

    pass


class Scraper:
    """Main scraper class for VRChat world metadata."""

    def __init__(
        self,
        database: Database,
        rate_limiter: RateLimiter,
        auth_cookie: str,
        image_storage_path: str,
    ):
        """Initialize scraper with dependencies."""
        self.db = database
        self.rate_limiter = rate_limiter
        self.auth_cookie = auth_cookie
        self.image_path = Path(image_storage_path)

        self.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=10),
            headers={"Cookie": f"auth={auth_cookie}"},
        )

    async def scrape_recent_worlds(self):
        """Fetch recently updated worlds and queue for scraping."""
        await self.rate_limiter.acquire()

        try:
            response = await self.client.get(
                "https://api.vrchat.cloud/api/1/worlds",
                params={"sort": "updated", "n": 1000},
            )
            response.raise_for_status()

            self.rate_limiter.record_result(True, response.status_code)

            worlds = [WorldSummary(**w) for w in response.json()]
            logger.info(f"Found {len(worlds)} recently updated worlds")

            # Queue all discovered worlds as PENDING
            for world in worlds:
                await self.db.upsert_world(
                    world.id,
                    {"discovered_at": datetime.utcnow().isoformat()},
                    status="PENDING",
                )

        except httpx.HTTPStatusError as e:
            self.rate_limiter.record_result(False, e.response.status_code)

            if e.response.status_code == 401:
                logger.error("Authentication failed - cookie may be expired")
                raise AuthenticationError("Cookie expired")
            else:
                logger.warning(f"Recent worlds request failed: {e}")

    async def scrape_world(self, world_id: str):
        """Scrape complete metadata for a single world."""
        await self.rate_limiter.acquire()

        try:
            response = await self.client.get(
                f"https://api.vrchat.cloud/api/1/worlds/{world_id}"
            )
            response.raise_for_status()

            self.rate_limiter.record_result(True, response.status_code)

            world = WorldDetail(**response.json())

            # Store stable metadata
            await self.db.upsert_world(
                world_id,
                world.stable_metadata(),
                status="SUCCESS",
            )

            # Store time-series metrics
            await self.db.insert_metrics(
                world_id,
                world.extract_metrics(),
                datetime.utcnow(),
            )

            logger.debug(f"Successfully scraped world {world_id}")

            # Queue image download if needed
            await self._queue_image_download(world)

        except httpx.HTTPStatusError as e:
            self.rate_limiter.record_result(False, e.response.status_code)

            if e.response.status_code == 404:
                logger.info(f"World {world_id} not found (deleted?)")
                await self.db.upsert_world(world_id, {}, status="DELETED")
            elif e.response.status_code == 401:
                logger.error("Authentication failed during world scrape")
                raise AuthenticationError("Cookie expired")
            else:
                logger.warning(f"Failed to scrape world {world_id}: {e}")

        except Exception as e:
            logger.error(f"Unexpected error scraping {world_id}: {e}")
            self.rate_limiter.record_result(False, None)

    async def _queue_image_download(self, world: WorldDetail):
        """Check if image exists, queue download if not."""
        image_path = self._get_image_path(world.id)

        if not image_path.exists():
            # For simplicity, download immediately
            try:
                await self._download_image(world.image_url, image_path)
            except Exception as e:
                logger.warning(f"Failed to download image for {world.id}: {e}")

    def _get_image_path(self, world_id: str) -> Path:
        """Generate hierarchical path for image storage."""
        # world_id format: wrld_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        uuid_part = world_id[5:]  # Remove 'wrld_' prefix
        return self.image_path / uuid_part[0:2] / uuid_part[2:4] / f"{world_id}.png"

    async def _download_image(self, url: str, path: Path):
        """Download image from URL to path."""
        path.parent.mkdir(parents=True, exist_ok=True)

        # Don't count image downloads against API rate limit
        response = await self.client.get(url)

        if response.status_code == 404:
            # Write 0-byte file to indicate intentional missing
            path.touch()
        else:
            response.raise_for_status()
            path.write_bytes(response.content)

    async def close(self):
        """Clean up resources."""
        await self.client.aclose()
