"""Core scraping logic for VRChat world metadata."""

import asyncio
import logging
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
        recent_worlds_interval: float = 3600,  # 1 hour
        idle_wait_time: float = 60,  # 1 minute
        error_backoff_time: float = 60,  # 1 minute
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
        self._recent_worlds_interval = recent_worlds_interval
        self._idle_wait_time = idle_wait_time
        self._error_backoff_time = error_backoff_time
        self._shutdown_event = asyncio.Event()
        self._task_group = asyncio.TaskGroup()

    async def run_forever(self):
        """Main entry point - runs scraping tasks forever."""
        async with self._task_group:
            self._task_group.create_task(self._scrape_recent_worlds_periodically())
            self._task_group.create_task(self._process_pending_worlds_continuously())
            self._task_group.create_task(self._process_pending_file_metadata_continuously())
            self._task_group.create_task(self._process_pending_image_downloads_continuously())

    def shutdown(self):
        """Signal the scraper to shutdown gracefully."""
        self._shutdown_event.set()

    def _create_task(self, coro):
        """Create a task, using TaskGroup if active or falling back to asyncio.create_task."""
        try:
            # Try to use the TaskGroup if it's active
            return self._task_group.create_task(coro)
        except RuntimeError:
            # TaskGroup not entered, fall back to regular create_task
            return asyncio.create_task(coro)

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
                self._create_task(self._scrape_recent_worlds_task())

                # Wait an hour before next discovery
                # Use injected sleep function for testability
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=0.1,  # Short timeout to check shutdown quickly
                    )
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    # Sleep for configured interval using injected sleep function
                    await self._sleep_func(self._recent_worlds_interval)
                    continue

            except Exception as e:
                logger.error(f"Error in recent worlds periodic task: {e}")
                await self._sleep_func(self._error_backoff_time)  # Back off on errors

    async def _process_pending_worlds_continuously(self):
        """Continuously process pending worlds from database."""
        while not self._shutdown_event.is_set():
            try:
                processed_count = await self._process_pending_worlds_batch(limit=100)
                if processed_count == 0:
                    await self._sleep_func(
                        self._idle_wait_time
                    )  # Wait before checking again

            except Exception as e:
                logger.error(f"Error in pending worlds processing: {e}")
                await self._sleep_func(self._error_backoff_time)  # Back off on errors

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

            # Note: Image downloads are now handled through the file metadata workflow
            # Files are queued as PENDING in the database, then processed separately

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

            # Download image - extract file_id from image_url for new protocol
            from .models import _parse_file_url, FileType
            file_ref = _parse_file_url(image_url, FileType.IMAGE)
            if file_ref:
                file_id = file_ref.file_id
                # For now, use a placeholder MD5 until we implement full file metadata workflow
                success, local_path, size, error = await self.image_downloader.download_image(
                    file_id, image_url, "placeholder_md5"
                )
                success = success  # Extract success from tuple
            else:
                success = False

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

    async def _scrape_file_metadata_task(self, file_id: str):
        """Handle file metadata scraping for a specific file."""
        request_id = f"file-{file_id}-{self._time_source()}"
        now = self._time_source()

        try:
            # Record request start
            self.api_rate_limiter.on_request_sent(request_id, now)

            # Make API call to get file metadata
            file_metadata = await self.api_client.get_file_metadata(file_id)

            # Record success
            self.api_rate_limiter.on_success(request_id, self._time_source())
            self.api_circuit_breaker.on_success()

            # Update database with file metadata (use aliases for consistency)
            await self.database.update_file_metadata(
                file_id,
                file_metadata.model_dump(mode="json", by_alias=True),
                status="SUCCESS"
            )

            logger.debug(f"Successfully scraped file metadata for {file_id}")

        except AuthenticationError:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            self.api_circuit_breaker.on_error(self._time_source())
            logger.critical("Authentication failed - shutting down")
            self.shutdown()
            raise

        except httpx.HTTPStatusError as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())

            if e.response.status_code == 404:
                await self.database.update_file_metadata(
                    file_id, {}, status="ERROR", error_message="File not found"
                )
                logger.info(f"File {file_id} not found (deleted?)")
            elif e.response.status_code in [429, 500, 502, 503]:
                self.api_circuit_breaker.on_error(self._time_source())
                logger.warning(f"Failed to scrape file metadata {file_id}: {e}")
            else:
                logger.warning(f"Failed to scrape file metadata {file_id}: {e}")

        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            self.api_circuit_breaker.on_error(self._time_source())
            logger.warning(f"Timeout scraping file metadata {file_id}: {e}")

        except Exception as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())
            logger.error(f"Unexpected error scraping file metadata {file_id}: {e}")

    async def _download_image_from_metadata_task(self, file_id: str, file_metadata_json: dict):
        """Handle image download based on file metadata."""
        # Wait until we can make an image request
        while True:
            delay = await self._get_image_request_delay()
            if delay <= 0:
                break
            await self._sleep_func(delay)

        request_id = f"image-metadata-{file_id}-{self._time_source()}"
        now = self._time_source()

        try:
            # Record request start
            self.image_rate_limiter.on_request_sent(request_id, now)

            # Parse file metadata to get download URL and MD5
            from .models import FileMetadata
            file_metadata = FileMetadata(**file_metadata_json)
            latest_version = file_metadata.get_latest_version()
            
            if not latest_version or not latest_version.file:
                await self.database.update_image_download(
                    file_id, "ERROR", error_message="No valid file version found"
                )
                return

            download_url = latest_version.file.url
            expected_md5 = latest_version.file.md5
            
            # Download image with MD5 verification
            success, local_path, size, error = await self.image_downloader.download_image(
                file_id, download_url, expected_md5
            )

            # Record result in rate limiter
            if success:
                self.image_rate_limiter.on_success(request_id, self._time_source())
                self.image_circuit_breaker.on_success()
                logger.debug(f"Successfully downloaded image for {file_id}")
            else:
                self.image_rate_limiter.on_error(request_id, self._time_source())
                logger.warning(f"Failed to download image for {file_id}: {error}")

            # Update database with download result
            if success:
                await self.database.update_image_download(
                    file_id, "SUCCESS", 
                    local_file_path=local_path,
                    downloaded_size_bytes=size
                )
            else:
                await self.database.update_image_download(
                    file_id, "ERROR", error_message=error
                )

        except Exception as e:
            self.image_rate_limiter.on_error(request_id, self._time_source())
            await self.database.update_image_download(
                file_id, "ERROR", error_message=str(e)
            )
            logger.error(f"Unexpected error downloading image for {file_id}: {e}")

    async def _process_pending_file_metadata_continuously(self):
        """Continuously process pending file metadata from database."""
        while not self._shutdown_event.is_set():
            try:
                processed_count = await self._process_pending_file_metadata_batch(limit=50)
                if processed_count == 0:
                    await self._sleep_func(
                        self._idle_wait_time
                    )  # Wait before checking again

            except Exception as e:
                logger.error(f"Error in pending file metadata processing: {e}")
                await self._sleep_func(self._error_backoff_time)  # Back off on errors

    async def _process_pending_image_downloads_continuously(self):
        """Continuously process pending image downloads from database."""
        while not self._shutdown_event.is_set():
            try:
                processed_count = await self._process_pending_image_downloads_batch(limit=20)
                if processed_count == 0:
                    await self._sleep_func(
                        self._idle_wait_time
                    )  # Wait before checking again

            except Exception as e:
                logger.error(f"Error in pending image downloads processing: {e}")
                await self._sleep_func(self._error_backoff_time)  # Back off on errors

    async def _process_pending_file_metadata_batch(self, limit: int = 50):
        """Execute one batch of pending file metadata processing.

        Args:
            limit: Maximum number of files to process in this batch

        Returns:
            Number of files that were processed
        """
        pending_files = await self.database.get_pending_file_metadata(limit=limit)
        if not pending_files:
            return 0

        batch_tasks = []
        for file_id, file_type in pending_files:
            if self._shutdown_event.is_set():
                break

            # Wait until we can make an API request
            while True:
                delay = await self._get_api_request_delay()
                if delay <= 0:
                    break
                await self._sleep_func(delay)

            # Launch individual file metadata scraping task
            task = self._create_task(self._scrape_file_metadata_task(file_id))
            batch_tasks.append(task)

        # Wait for all tasks in this batch to complete before returning
        if batch_tasks:
            await asyncio.gather(*batch_tasks, return_exceptions=True)

        return len(batch_tasks)

    async def _process_pending_image_downloads_batch(self, limit: int = 20):
        """Execute one batch of pending image downloads processing.

        Args:
            limit: Maximum number of images to process in this batch

        Returns:
            Number of images that were processed
        """
        pending_images = await self.database.get_pending_image_downloads(limit=limit)
        if not pending_images:
            return 0

        batch_tasks = []
        for file_id, file_metadata_json, file_type in pending_images:
            if self._shutdown_event.is_set():
                break

            # Wait until we can make an image request
            while True:
                delay = await self._get_image_request_delay()
                if delay <= 0:
                    break
                await self._sleep_func(delay)

            # Launch individual image download task
            task = self._create_task(
                self._download_image_from_metadata_task(file_id, file_metadata_json)
            )
            batch_tasks.append(task)

        # Wait for all tasks in this batch to complete before returning
        if batch_tasks:
            await asyncio.gather(*batch_tasks, return_exceptions=True)

        return len(batch_tasks)

    async def _update_database_with_world(
        self, world_id: str, world_details: WorldDetail
    ):
        """Update database with world metadata and metrics."""
        # Use new transactional method to store world + discovered files + metrics
        await self.database.upsert_world_with_files(
            world_id,
            world_details.stable_metadata(),
            world_details.extract_metrics(),
            world_details.discovered_files,
            status="SUCCESS",
        )

    async def _scrape_recent_worlds_batch(self):
        """Execute one round of recent worlds discovery and await completion.

        This is the extracted inner logic from _scrape_recent_worlds_periodically
        without the timing/sleep aspects, useful for testing.
        """
        # Wait until we can make an API request
        while True:
            delay = await self._get_api_request_delay()
            if delay <= 0:
                break
            await self._sleep_func(delay)

        # Launch recent worlds discovery task and await it
        await self._scrape_recent_worlds_task()

    async def _process_pending_worlds_batch(self, limit: int = 100):
        """Execute one batch of pending worlds processing and await all tasks.

        This is the extracted inner logic from _process_pending_worlds_continuously
        without the timing/sleep aspects, useful for testing.

        Args:
            limit: Maximum number of worlds to process in this batch

        Returns:
            Number of worlds that were processed
        """
        worlds = await self.database.get_worlds_to_scrape(limit=limit)
        if not worlds:
            return 0

        batch_tasks = []
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
            task = self._create_task(self._scrape_world_task(world_id))
            batch_tasks.append(task)

        # Wait for all tasks in this batch to complete before returning
        if batch_tasks:
            await asyncio.gather(*batch_tasks, return_exceptions=True)

        return len(batch_tasks)

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
