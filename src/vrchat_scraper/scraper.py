"""Core scraping logic for VRChat world metadata."""

import asyncio
import logging
import sys
import time
from datetime import datetime
from typing import Awaitable, Callable

import httpx

from .circuit_breaker import CircuitBreaker
from .database import Database
from .http_client import AuthenticationError
from .models import WorldDetail
from .protocols import ImageDownloader, VRChatAPIClient
from .rate_limiter import BBRRateLimiter

logger = logging.getLogger(__name__)


class VRChatScraper:
    """Main scraper coordinator for VRChat world metadata."""

    def __init__(
        self,
        database: Database,
        api_client: VRChatAPIClient,
        image_downloader: ImageDownloader,
        api_rate_limiter: BBRRateLimiter,
        image_rate_limiter: BBRRateLimiter,
        api_circuit_breaker: CircuitBreaker,
        image_circuit_breaker: CircuitBreaker,
        time_source: Callable[[], float] = time.time,
        sleep_func: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        """Initialize scraper with all dependencies."""
        self.database = database
        self.api_client = api_client
        self.image_downloader = image_downloader
        self.api_rate_limiter = api_rate_limiter
        self.image_rate_limiter = image_rate_limiter
        self.api_circuit_breaker = api_circuit_breaker
        self.image_circuit_breaker = image_circuit_breaker
        self._time_source = time_source
        self._sleep_func = sleep_func
        self._shutdown_event = asyncio.Event()
        self._task_group: asyncio.TaskGroup | None = None

    async def run_forever(self):
        """Main entry point - runs scraping tasks forever."""
        if sys.version_info >= (3, 11):
            async with asyncio.TaskGroup() as tg:
                self._task_group = tg
                tg.create_task(self._scrape_recent_worlds_periodically())
                tg.create_task(self._process_pending_worlds_continuously())
        else:
            await asyncio.gather(
                self._scrape_recent_worlds_periodically(),
                self._process_pending_worlds_continuously(),
            )

    def shutdown(self):
        """Signal the scraper to shutdown gracefully."""
        self._shutdown_event.set()

    async def _scrape_recent_worlds_periodically(self):
        """Discover new worlds via recent API every hour."""
        while not self._shutdown_event.is_set():
            try:
                # Wait until we can make an API request
                while True:
                    delay = await self._get_api_request_delay()
                    if delay <= 0:
                        break
                    await self._sleep_func(delay)

                # Launch recent worlds discovery task
                if self._task_group:
                    self._task_group.create_task(self._scrape_recent_worlds_task())
                else:
                    asyncio.create_task(self._scrape_recent_worlds_task())

                # Wait an hour before next discovery
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=3600)
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue loop

            except Exception as e:
                logger.error(f"Error in recent worlds periodic task: {e}")
                await self._sleep_func(60)  # Back off on errors

    async def _process_pending_worlds_continuously(self):
        """Continuously process pending worlds from database."""
        while not self._shutdown_event.is_set():
            try:
                worlds = await self.database.get_worlds_to_scrape(limit=100)
                if not worlds:
                    await self._sleep_func(60)  # Wait before checking again
                    continue

                for world_id in worlds:
                    if self._shutdown_event.is_set():
                        break

                    # Wait until we can make an API request
                    while True:
                        delay = await self._get_api_request_delay()
                        if delay <= 0:
                            break
                        await self._sleep_func(delay)

                    # Launch individual world scraping task
                    if self._task_group:
                        self._task_group.create_task(self._scrape_world_task(world_id))
                    else:
                        asyncio.create_task(self._scrape_world_task(world_id))

            except Exception as e:
                logger.error(f"Error in pending worlds processing: {e}")
                await self._sleep_func(60)  # Back off on errors

    async def _get_api_request_delay(self) -> float:
        """Check both circuit breaker and rate limiter for API requests."""
        now = self._time_source()

        # Circuit breaker takes priority
        cb_delay = self.api_circuit_breaker.get_delay_until_proceed(now)
        if cb_delay > 0:
            return cb_delay

        # Then rate limiter
        rl_delay = self.api_rate_limiter.get_delay_until_next_request(now)
        return rl_delay

    async def _get_image_request_delay(self) -> float:
        """Check both circuit breaker and rate limiter for image requests."""
        now = self._time_source()

        cb_delay = self.image_circuit_breaker.get_delay_until_proceed(now)
        if cb_delay > 0:
            return cb_delay

        rl_delay = self.image_rate_limiter.get_delay_until_next_request(now)
        return rl_delay

    async def _scrape_recent_worlds_task(self):
        """Handle recent worlds discovery."""
        request_id = f"recent-{self._time_source()}"
        now = self._time_source()

        try:
            # Record request start
            self.api_rate_limiter.on_request_sent(request_id, now)

            # Make API call
            worlds = await self.api_client.get_recent_worlds()

            # Record success
            self.api_rate_limiter.on_success(request_id, self._time_source())
            self.api_circuit_breaker.on_success()

            logger.info(f"Found {len(worlds)} recently updated worlds")

            # Queue all discovered worlds as PENDING
            for world in worlds:
                await self.database.upsert_world(
                    world.id,
                    {"discovered_at": datetime.utcnow().isoformat()},
                    status="PENDING",
                )

        except AuthenticationError:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            self.api_circuit_breaker.on_error(self._time_source())
            logger.critical("Authentication failed - shutting down")
            self.shutdown()
            raise

        except httpx.HTTPStatusError as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())

            if e.response.status_code in [429, 500, 502, 503]:
                self.api_circuit_breaker.on_error(self._time_source())

            logger.warning(f"Recent worlds request failed: {e}")

        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            self.api_circuit_breaker.on_error(self._time_source())
            logger.warning(f"Timeout in recent worlds request: {e}")

        except Exception as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            logger.error(f"Unexpected error in recent worlds task: {e}")

    async def _scrape_world_task(self, world_id: str):
        """Handle complete world scraping: metadata + image."""
        request_id = f"world-{world_id}-{self._time_source()}"
        now = self._time_source()

        try:
            # Record request start
            self.api_rate_limiter.on_request_sent(request_id, now)

            # Make API call
            world_details = await self.api_client.get_world_details(world_id)

            # Record success
            self.api_rate_limiter.on_success(request_id, self._time_source())
            self.api_circuit_breaker.on_success()

            # Update database immediately
            await self._update_database_with_world(world_id, world_details)

            # Launch image download if needed
            if world_details.image_url and not await self._image_exists(world_id):
                if self._task_group:
                    self._task_group.create_task(
                        self._download_image_task(world_id, world_details.image_url)
                    )
                else:
                    asyncio.create_task(
                        self._download_image_task(world_id, world_details.image_url)
                    )

            logger.debug(f"Successfully scraped world {world_id}")

        except AuthenticationError:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            self.api_circuit_breaker.on_error(self._time_source())
            logger.critical("Authentication failed - shutting down")
            self.shutdown()
            raise

        except httpx.HTTPStatusError as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())

            if e.response.status_code == 404:
                await self._mark_world_deleted(world_id)
                logger.info(f"World {world_id} not found (deleted?)")
            elif e.response.status_code in [429, 500, 502, 503]:
                self.api_circuit_breaker.on_error(self._time_source())
                logger.warning(f"Failed to scrape world {world_id}: {e}")
            else:
                logger.warning(f"Failed to scrape world {world_id}: {e}")

        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            self.api_circuit_breaker.on_error(self._time_source())
            logger.warning(f"Timeout scraping world {world_id}: {e}")

        except Exception as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            logger.error(f"Unexpected error scraping {world_id}: {e}")

    async def _download_image_task(self, world_id: str, image_url: str):
        """Handle image download with separate rate limiting."""
        # Wait until we can make an image request
        while True:
            delay = await self._get_image_request_delay()
            if delay <= 0:
                break
            await self._sleep_func(delay)

        request_id = f"image-{world_id}-{self._time_source()}"
        now = self._time_source()

        try:
            # Record request start
            self.image_rate_limiter.on_request_sent(request_id, now)

            # Download image
            success = await self.image_downloader.download_image(image_url, world_id)

            # Record result
            if success:
                self.image_rate_limiter.on_success(request_id, self._time_source())
                self.image_circuit_breaker.on_success()
                logger.debug(f"Successfully downloaded image for {world_id}")
            else:
                self.image_rate_limiter.on_error(request_id, self._time_source())
                logger.warning(f"Failed to download image for {world_id}")

        except Exception as e:
            self.image_rate_limiter.on_error(request_id, self._time_source())
            logger.error(f"Unexpected error downloading image for {world_id}: {e}")

    async def _update_database_with_world(
        self, world_id: str, world_details: WorldDetail
    ):
        """Update database with world metadata and metrics."""
        # Store stable metadata
        await self.database.upsert_world(
            world_id,
            world_details.stable_metadata(),
            status="SUCCESS",
        )

        # Store time-series metrics
        await self.database.insert_metrics(
            world_id,
            world_details.extract_metrics(),
            datetime.utcnow(),
        )

    async def _mark_world_deleted(self, world_id: str):
        """Mark a world as deleted in the database."""
        await self.database.upsert_world(world_id, {}, status="DELETED")

    async def _image_exists(self, world_id: str) -> bool:
        """Check if image already exists for a world."""
        # Assume image downloader can check this
        if hasattr(self.image_downloader, "image_exists"):
            return self.image_downloader.image_exists(world_id)
        return False

    async def close(self):
        """Clean up resources."""
        if hasattr(self.api_client, "close"):
            await self.api_client.close()
        if hasattr(self.image_downloader, "close"):
            await self.image_downloader.close()
