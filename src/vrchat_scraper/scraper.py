"""Core scraping logic for VRChat world metadata."""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Awaitable, Callable

import httpx
import logfire

from .circuit_breaker import CircuitBreaker, CircuitBreakerState
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
        recent_worlds_interval: float = 600,
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
        # used for fairness; the rate limiter is sort of a semaphore already
        # but the sleep waiting doesn't seem to be fair (some tasks can starve for a while)
        self.api_lock = asyncio.Lock()
        self.image_lock = asyncio.Lock()
        self._time_source = time_source
        self._sleep_func = sleep_func
        self._recent_worlds_interval = recent_worlds_interval
        self._idle_wait_time = idle_wait_time
        self._error_backoff_time = error_backoff_time
        self._shutdown_event = asyncio.Event()

        # --- Phase 2 Observability: Business Metrics ---
        self._worlds_scraped_counter = logfire.metric_counter("scraper.worlds_scraped")
        self._files_processed_counter = logfire.metric_counter(
            "scraper.files_processed"
        )
        self._images_downloaded_counter = logfire.metric_counter(
            "scraper.images_downloaded"
        )
        self._api_errors_counter = logfire.metric_counter("scraper.api_errors")
        self._image_errors_counter = logfire.metric_counter("scraper.image_errors")

        # Queue depth gauges (callback-based for real-time monitoring)
        self._pending_worlds_gauge = logfire.metric_gauge(
            "scraper.queue.pending_worlds"
        )
        self._pending_files_gauge = logfire.metric_gauge(
            "scraper.queue.pending_file_metadata"
        )
        self._pending_images_gauge = logfire.metric_gauge(
            "scraper.queue.pending_image_downloads"
        )

        # Start periodic queue depth monitoring
        self._queue_monitor_task = None

    async def run_forever(self):
        """Main entry point - runs scraping tasks forever."""
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(self._scrape_recent_worlds_periodically())
            task_group.create_task(self._process_pending_worlds_continuously())
            task_group.create_task(self._process_pending_file_metadata_continuously())
            task_group.create_task(self._process_pending_image_downloads_continuously())
            task_group.create_task(self._monitor_queue_depths_periodically())

    async def run_oneshot(self):
        """One-shot execution - process all pending work and exit."""
        logger.info("Starting one-shot scraping run")

        try:
            # Step 1: Scrape recent worlds to discover new ones
            logger.info("Scraping recent worlds...")
            await self._scrape_recent_worlds_batch()

            # Step 2: Process all pending worlds until queue is empty
            logger.info("Processing pending worlds...")
            while True:
                processed_count = await self._process_pending_worlds_batch(limit=100)
                if processed_count == 0:
                    break
                logger.info(f"Processed {processed_count} worlds")

            # Step 3: Process all pending file metadata until queue is empty
            logger.info("Processing pending file metadata...")
            while True:
                processed_count = await self._process_pending_file_metadata_batch(
                    limit=50
                )
                if processed_count == 0:
                    break
                logger.info(f"Processed {processed_count} file metadata entries")

            # Step 4: Process all pending image downloads until queue is empty
            logger.info("Processing pending image downloads...")
            while True:
                processed_count = await self._process_pending_image_downloads_batch(
                    limit=20
                )
                if processed_count == 0:
                    break
                logger.info(f"Processed {processed_count} image downloads")

            logger.info("One-shot scraping run completed successfully")

        except Exception as e:
            logger.error(f"Error during one-shot run: {e}")
            raise

    def shutdown(self):
        """Signal the scraper to shutdown gracefully."""
        self._shutdown_event.set()

    def _classify_http_error(self, status_code: int) -> str:
        """Classify HTTP errors into categories for structured observability."""
        if status_code == 401:
            return "authentication_error"
        elif status_code == 403:
            return "authorization_error"
        elif status_code == 404:
            return "not_found_error"
        elif status_code == 429:
            return "rate_limit_error"
        elif 400 <= status_code < 500:
            return "client_error"
        elif 500 <= status_code < 600:
            return "server_error"
        else:
            return "unknown_http_error"

    def _is_rate_limiting_error(self, status_code: int) -> bool:
        """Determine if an HTTP status code indicates rate limiting or server congestion.

        Returns True for errors that should trigger rate limiter backoff:
        - 429: Explicit rate limit signal
        - 500/502/503: Server errors indicating overload

        Returns False for benign errors that don't indicate congestion:
        - 404/410: Resource not found/deleted (expected in archival context)
        - 401/403: Authentication/authorization issues
        - Other 4xx: Client errors, not server capacity issues
        """
        return status_code in [429, 500, 502, 503]

    async def _scrape_recent_worlds_periodically(self):
        """Discover new worlds via recent API at a fixed interval."""
        while not self._shutdown_event.is_set():
            try:
                # Launch recent worlds discovery task
                await self._scrape_recent_worlds_task()

                # Wait before next discovery (configurable interval)
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

    @logfire.instrument("wait_for_api_ready")
    async def _wait_for_api_ready(self):
        """Wait until API requests are allowed by rate limiter and circuit breaker."""
        # Check circuit breaker state before trying to avoid wasting time
        # Only fail fast if backoff is very long (>60s)
        now = self._time_source()
        cb_delay = self.api_circuit_breaker.get_delay_until_proceed(now)
        if cb_delay > 60.0:
            cb_state = self.api_circuit_breaker.state.name
            cb_backoff = self.api_circuit_breaker._backoff_duration
            raise asyncio.TimeoutError(
                f"Circuit breaker backoff too long ({cb_delay:.1f}s remaining, "
                f"state: {cb_state}, backoff: {cb_backoff:.1f}s)"
            )

        total_wait = 0.0
        iteration = 0

        while True:
            iteration += 1

            # Atomically check delay and conditionally proceed
            # This must be atomic to prevent race conditions
            async with self.api_lock:
                delay = await self._get_api_request_delay()

                # If delay is small enough, proceed immediately
                if delay < 0.01:
                    break

                # If delay is too large, give up without sleeping
                cb_state = self.api_circuit_breaker.state
                max_wait = 300.0 if cb_state == CircuitBreakerState.CLOSED else 60.0

                if total_wait + delay > max_wait:
                    cb_backoff = self.api_circuit_breaker._backoff_duration
                    rl_rate = self.api_rate_limiter._get_effective_rate()
                    rl_inflight = self.api_rate_limiter.inflight
                    raise asyncio.TimeoutError(
                        f"Wait time exceeded {max_wait:.0f}s: "
                        f"total_wait={total_wait:.1f}s, next_delay={delay:.1f}s, iterations={iteration}, "
                        f"CB[state={cb_state.name}, backoff={cb_backoff:.1f}s], "
                        f"RL[rate={rl_rate:.2f}/s, inflight={rl_inflight}]"
                    )

                # For reasonable delays, sleep a shorter time and re-check
                # This prevents holding the lock during sleep while avoiding race conditions
                # Sleep for at most 100ms before re-checking
                sleep_time = min(delay, 0.1)

                # Log if we're waiting for circuit breaker OR if we're doing many iterations
                if delay > 5.0 or (iteration > 10 and iteration % 50 == 0):
                    cb_backoff = self.api_circuit_breaker._backoff_duration
                    rl_rate = self.api_rate_limiter._get_effective_rate()
                    rl_inflight = self.api_rate_limiter.inflight
                    delay_str = f"{delay:.6f}s" if delay < 0.1 else f"{delay:.1f}s"
                    logger.info(
                        f"Waiting {delay_str} for API (iteration {iteration}, "
                        f"total_wait: {total_wait:.1f}s, CB: {cb_state.name}, "
                        f"RL rate: {rl_rate:.2f}/s, inflight: {rl_inflight})"
                    )

            # Sleep WITHOUT holding lock, but only for a short time
            # This allows other tasks to make progress while avoiding long lock holds
            await self._sleep_func(sleep_time)
            total_wait += sleep_time

    @logfire.instrument("wait_for_iamge_ready")
    async def _wait_for_image_ready(self):
        """Wait until image requests are allowed by rate limiter and circuit breaker."""
        # Check circuit breaker state before trying to avoid wasting time
        # Only fail fast if backoff is very long (>60s)
        now = self._time_source()
        cb_delay = self.image_circuit_breaker.get_delay_until_proceed(now)
        if cb_delay > 60.0:
            cb_state = self.image_circuit_breaker.state.name
            cb_backoff = self.image_circuit_breaker._backoff_duration
            raise asyncio.TimeoutError(
                f"Image circuit breaker backoff too long ({cb_delay:.1f}s remaining, "
                f"state: {cb_state}, backoff: {cb_backoff:.1f}s)"
            )

        total_wait = 0.0
        iteration = 0

        while True:
            iteration += 1

            # Atomically check delay and conditionally proceed
            async with self.image_lock:
                delay = await self._get_image_request_delay()

                # If delay is small enough, proceed immediately
                if delay < 0.01:
                    break

                # If delay is too large, give up without sleeping
                cb_state = self.image_circuit_breaker.state
                max_wait = 300.0 if cb_state == CircuitBreakerState.CLOSED else 60.0

                if total_wait + delay > max_wait:
                    cb_backoff = self.image_circuit_breaker._backoff_duration
                    rl_rate = self.image_rate_limiter._get_effective_rate()
                    rl_inflight = self.image_rate_limiter.inflight
                    raise asyncio.TimeoutError(
                        f"Wait time exceeded {max_wait:.0f}s: "
                        f"total_wait={total_wait:.1f}s, next_delay={delay:.1f}s, "
                        f"CB[state={cb_state.name}, backoff={cb_backoff:.1f}s], "
                        f"RL[rate={rl_rate:.2f}/s, inflight={rl_inflight}]"
                    )

                # Sleep for at most 100ms before re-checking
                sleep_time = min(delay, 0.1)

                # Log if we're waiting for circuit breaker
                if delay > 5.0 or (iteration > 10 and iteration % 50 == 0):
                    cb_backoff = self.image_circuit_breaker._backoff_duration
                    rl_rate = self.image_rate_limiter._get_effective_rate()
                    rl_inflight = self.image_rate_limiter.inflight
                    delay_str = f"{delay:.6f}s" if delay < 0.1 else f"{delay:.1f}s"
                    logger.info(
                        f"Waiting {delay_str} for image API (iteration {iteration}, "
                        f"total_wait: {total_wait:.1f}s, CB: {cb_state.name}, "
                        f"RL rate: {rl_rate:.2f}/s, inflight: {rl_inflight})"
                    )

            # Sleep WITHOUT holding lock, but only for a short time
            await self._sleep_func(sleep_time)
            total_wait += sleep_time

    async def _execute_api_call(
        self,
        request_prefix: str,
        api_call: Callable[[], Awaitable[Any]],
        error_context: str,
    ) -> Any:
        """Execute an API call with standard rate limiting, circuit breaking, and error handling.

        Args:
            request_prefix: Prefix for request ID generation
            api_call: Async function that makes the API call and returns result
            error_context: Context string for error logging

        Returns:
            The result of the API call

        Raises:
            AuthenticationError: On authentication failures (also triggers shutdown)
            httpx.HTTPStatusError: On HTTP errors (after recording metrics)
            httpx.TimeoutException: On timeout errors (after recording metrics)
            Exception: On other unexpected errors (after recording metrics)
        """
        request_id = f"{request_prefix}-{self._time_source()}"
        now = self._time_source()

        try:
            # Record request start
            self.api_rate_limiter.on_request_sent(request_id, now)

            # Make API call
            result = await api_call()

            # Record success
            self.api_rate_limiter.on_success(request_id, self._time_source())
            self.api_circuit_breaker.on_success()

            return result

        except AuthenticationError:
            now = self._time_source()
            self.api_rate_limiter.on_error(request_id, now)
            logger.warning(
                f"[CB TRIGGER] Authentication error in {error_context}, "
                f"triggering circuit breaker (request_id: {request_id})"
            )
            self.api_circuit_breaker.on_error(now)

            # Enhanced error tracking
            with logfire.span("authentication_error", error_context=error_context):
                logfire.error(
                    "Authentication failed - shutting down",
                    error_type="authentication_error",
                    request_id=request_id,
                    context=error_context,
                )

            logger.critical("Authentication failed - shutting down")
            self.shutdown()
            raise

        except httpx.HTTPStatusError as e:
            # Only report rate-limiting errors to rate limiter
            # Benign errors (404, 401, 403) don't indicate API congestion
            if self._is_rate_limiting_error(e.response.status_code):
                now = self._time_source()
                self.api_rate_limiter.on_error(request_id, now)
                try:
                    url = str(e.request.url)
                except (AttributeError, RuntimeError):
                    url = "unknown"
                logger.warning(
                    f"[CB TRIGGER] HTTP {e.response.status_code} in {error_context}, "
                    f"triggering circuit breaker (request_id: {request_id}, url: {url})"
                )
                self.api_circuit_breaker.on_error(now)
            else:
                # Benign error (404, 401, 403) - request completed successfully from rate limiter perspective
                # Record as success so inflight decrements properly
                self.api_rate_limiter.on_success(request_id, self._time_source())
                logger.debug(
                    f"[CB SKIP] HTTP {e.response.status_code} in {error_context}, "
                    f"NOT triggering circuit breaker (benign error, marked as success for rate limiter)"
                )

            # Enhanced error tracking with structured context
            error_type = self._classify_http_error(e.response.status_code)
            with logfire.span("http_error", error_context=error_context):
                logfire.error(
                    f"{error_context} failed with HTTP error",
                    error_type=error_type,
                    status_code=e.response.status_code,
                    request_id=request_id,
                    context=error_context,
                    response_headers=dict(e.response.headers),
                )

            logger.warning(f"{error_context} failed: {e}")
            raise

        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            now = self._time_source()
            self.api_rate_limiter.on_error(request_id, now)

            # Enhanced error tracking
            timeout_type = type(e).__name__
            logger.warning(
                f"[CB TRIGGER] {timeout_type} in {error_context}, "
                f"triggering circuit breaker (request_id: {request_id})"
            )
            self.api_circuit_breaker.on_error(now)
            with logfire.span("timeout_error", error_context=error_context):
                logfire.error(
                    f"Timeout in {error_context}",
                    error_type="timeout_error",
                    timeout_type=timeout_type,
                    request_id=request_id,
                    context=error_context,
                )

            logger.warning(f"Timeout in {error_context}: {e}")
            raise

        except Exception as e:
            self.api_rate_limiter.on_error(request_id, self._time_source())

            # Enhanced error tracking for unexpected errors
            with logfire.span("unexpected_error", error_context=error_context):
                logfire.error(
                    f"Unexpected error in {error_context}",
                    error_type="unexpected_error",
                    exception_type=type(e).__name__,
                    exception_message=str(e),
                    request_id=request_id,
                    context=error_context,
                )

            logger.error(f"Unexpected error in {error_context}: {e}")
            raise

    @logfire.instrument("scrape_recent_worlds_task")
    async def _scrape_recent_worlds_task(self):
        """Handle recent worlds discovery."""
        try:
            await self._wait_for_api_ready()
            worlds = await self._execute_api_call(
                request_prefix="recent",
                api_call=self.api_client.get_recent_worlds,
                error_context="recent worlds request",
            )

            logger.info(f"Found {len(worlds)} recently updated worlds")
            # Queue all discovered worlds as PENDING in batch
            if worlds:
                discovery_time = datetime.utcnow().isoformat()
                worlds_data = [
                    (world.id, {"discovered_at": discovery_time}, "PENDING")
                    for world in worlds
                ]
                await self.database.batch_upsert_worlds(worlds_data)
        except httpx.HTTPStatusError:
            # Let HTTP errors propagate - they're already logged by _execute_api_call
            pass
        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout):
            # Let timeout errors propagate - they're already logged by _execute_api_call
            pass

    @logfire.instrument("scrape_world_task")
    async def _scrape_world_task(self, world_id: str):
        """Handle complete world scraping: metadata + image."""
        try:
            await self._wait_for_api_ready()

            world_details = await self._execute_api_call(
                request_prefix=f"world-{world_id}",
                api_call=lambda: self.api_client.get_world_details(world_id),
                error_context=f"world {world_id} scraping",
            )

            # Update database immediately with timeout
            try:
                await asyncio.wait_for(
                    self._update_database_with_world(world_id, world_details),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"Database operation timed out updating world {world_id}, "
                    "will retry on next scrape cycle"
                )
                self._worlds_scraped_counter.add(1, {"status": "db_timeout"})
                return

            # Note: Image downloads are now handled through the file metadata workflow
            # Files are queued as PENDING in the database, then processed separately
            logger.debug(f"Successfully scraped world {world_id}")

            # Record business metric
            self._worlds_scraped_counter.add(1, {"status": "success"})

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Mark world as deleted with timeout protection
                try:
                    await asyncio.wait_for(
                        self._mark_world_deleted(world_id),
                        timeout=30.0,
                    )
                    logger.info(f"World {world_id} not found (deleted?)")
                    self._worlds_scraped_counter.add(1, {"status": "deleted"})
                except asyncio.TimeoutError:
                    logger.error(
                        f"Database operation timed out marking world {world_id} as deleted, "
                        "will retry on next scrape cycle"
                    )
                    self._worlds_scraped_counter.add(1, {"status": "db_timeout"})
                except Exception as db_error:
                    logger.error(
                        f"Database error marking world {world_id} as deleted: {db_error}",
                        exc_info=True,
                    )
                    self._worlds_scraped_counter.add(1, {"status": "db_error"})
            else:
                # Log but continue - circuit breaker handles rate limits/server errors
                self._worlds_scraped_counter.add(1, {"status": "error"})
                self._api_errors_counter.add(
                    1,
                    {
                        "error_type": "http_error",
                        "status_code": str(e.response.status_code),
                    },
                )
                logger.warning(
                    f"HTTP error scraping world {world_id}: {e.response.status_code}"
                )

        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            # Transient network errors - safe to continue
            self._worlds_scraped_counter.add(1, {"status": "timeout"})
            self._api_errors_counter.add(1, {"error_type": "timeout"})
            logger.warning(f"Timeout scraping world {world_id}: {e}")

        except asyncio.TimeoutError as e:
            # Circuit breaker backoff too long or other timeout
            self._worlds_scraped_counter.add(1, {"status": "wait_timeout"})
            logger.warning(
                f"Timed out waiting for API to be ready for world {world_id}: {e}"
            )

        except AuthenticationError:
            # Re-raise authentication errors - they should propagate to trigger shutdown
            raise

        except Exception as e:
            # Catch any database errors or other unexpected exceptions
            # Log but don't let them crash the entire batch
            logger.error(
                f"Unexpected error scraping world {world_id}: {e}", exc_info=True
            )
            self._worlds_scraped_counter.add(1, {"status": "unexpected_error"})
            self._api_errors_counter.add(
                1, {"error_type": "unexpected_error", "exception": type(e).__name__}
            )

    @logfire.instrument("scrape_file_metadata_task")
    async def _scrape_file_metadata_task(self, file_id: str):
        """Handle file metadata scraping for a specific file."""
        try:
            await self._wait_for_api_ready()

            file_metadata = await self._execute_api_call(
                request_prefix=f"file-{file_id}",
                api_call=lambda: self.api_client.get_file_metadata(file_id),
                error_context=f"file metadata {file_id} scraping",
            )

            # Store raw file metadata directly with timeout protection
            try:
                await asyncio.wait_for(
                    self.database.update_file_metadata(
                        file_id,
                        file_metadata,
                        status="SUCCESS",
                    ),
                    timeout=30.0,
                )
                logger.debug(f"Successfully scraped file metadata for {file_id}")
                self._files_processed_counter.add(1, {"status": "success"})
            except asyncio.TimeoutError:
                logger.error(
                    f"Database operation timed out updating file {file_id}, "
                    "will retry on next scrape cycle"
                )
                self._files_processed_counter.add(1, {"status": "db_timeout"})

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                try:
                    await asyncio.wait_for(
                        self.database.update_file_metadata(
                            file_id, {}, status="ERROR", error_message="File not found"
                        ),
                        timeout=30.0,
                    )
                    logger.info(f"File {file_id} not found (deleted?)")
                    self._files_processed_counter.add(1, {"status": "not_found"})
                except (asyncio.TimeoutError, Exception) as db_error:
                    logger.error(
                        f"Database error marking file {file_id} as not found: {db_error}"
                    )
                    self._files_processed_counter.add(1, {"status": "db_error"})
            else:
                self._files_processed_counter.add(1, {"status": "error"})
                self._api_errors_counter.add(
                    1,
                    {
                        "error_type": "http_error",
                        "status_code": str(e.response.status_code),
                    },
                )
            # Other HTTP errors are already logged by _execute_api_call
        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout):
            self._files_processed_counter.add(1, {"status": "timeout"})
            self._api_errors_counter.add(1, {"error_type": "timeout"})
            # Timeout errors are already logged by _execute_api_call
        except asyncio.TimeoutError as e:
            # Circuit breaker backoff too long
            self._files_processed_counter.add(1, {"status": "wait_timeout"})
            logger.warning(
                f"Timed out waiting for API to be ready for file {file_id}: {e}"
            )
        except Exception as e:
            logger.error(
                f"Unexpected error scraping file {file_id}: {e}", exc_info=True
            )
            self._files_processed_counter.add(1, {"status": "unexpected_error"})

    @logfire.instrument("download_image_content_task")
    async def _download_image_content_task(
        self,
        file_id: str,
        version: int,
        filename: str,
        md5: str,
        size_bytes: int,
        download_url: str,
    ):
        """Handle image download using content-addressed storage."""

        request_id = f"image-content-{file_id}-v{version}-{self._time_source()}"
        now = self._time_source()

        try:
            await self._wait_for_image_ready()
            self.image_rate_limiter.on_request_sent(request_id, now)

            # Download image with dual hash verification
            result = await self.image_downloader.download_image(
                file_id, version, download_url, md5, size_bytes
            )

            # Record result in rate limiter
            if result.success:
                self.image_rate_limiter.on_success(request_id, self._time_source())
                self.image_circuit_breaker.on_success()
                logger.debug(
                    f"Successfully downloaded image for {file_id} v{version} (SHA256: {result.sha256_hash})"
                )

                # Record business metric
                self._images_downloaded_counter.add(1, {"status": "success"})
            else:
                self.image_rate_limiter.on_error(request_id, self._time_source())
                logger.warning(
                    f"Failed to download image for {file_id} v{version}: {result.error_message}"
                )

                # Record business metric
                self._images_downloaded_counter.add(1, {"status": "error"})
                self._image_errors_counter.add(1, {"error_type": "download_failed"})

            # Update database with download result (with timeout protection)
            try:
                if result.success:
                    await asyncio.wait_for(
                        self.database.update_image_download(
                            file_id,
                            version,
                            "CONFIRMED",
                            sha256=result.sha256_hash,
                        ),
                        timeout=30.0,
                    )
                else:
                    await asyncio.wait_for(
                        self.database.update_image_download(
                            file_id,
                            version,
                            "ERROR",
                            error_message=result.error_message,
                        ),
                        timeout=30.0,
                    )
            except asyncio.TimeoutError:
                logger.error(
                    f"Database operation timed out updating image {file_id} v{version}, "
                    "will retry on next scrape cycle"
                )

        except asyncio.TimeoutError as e:
            # Circuit breaker backoff too long
            self._images_downloaded_counter.add(1, {"status": "wait_timeout"})
            logger.warning(
                f"Timed out waiting for image API to be ready for {file_id} v{version}: {e}"
            )

        except Exception as e:
            self.image_rate_limiter.on_error(request_id, self._time_source())
            try:
                await asyncio.wait_for(
                    self.database.update_image_download(
                        file_id, version, "ERROR", error_message=str(e)
                    ),
                    timeout=30.0,
                )
            except (asyncio.TimeoutError, Exception) as db_error:
                logger.error(
                    f"Database error recording image download failure for {file_id} v{version}: {db_error}"
                )

            logger.error(
                f"Unexpected error downloading image for {file_id} v{version}: {e}"
            )

            # Record business metrics
            self._images_downloaded_counter.add(1, {"status": "error"})
            self._image_errors_counter.add(1, {"error_type": "unexpected_error"})

    async def _process_pending_file_metadata_continuously(self):
        """Continuously process pending file metadata from database."""
        while not self._shutdown_event.is_set():
            try:
                processed_count = await self._process_pending_file_metadata_batch(
                    limit=50
                )
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
                processed_count = await self._process_pending_image_downloads_batch(
                    limit=20
                )
                if processed_count == 0:
                    await self._sleep_func(
                        self._idle_wait_time
                    )  # Wait before checking again

            except Exception as e:
                logger.error(f"Error in pending image downloads processing: {e}")
                await self._sleep_func(self._error_backoff_time)  # Back off on errors

    @logfire.instrument("process_pending_file_metadata_batch")
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

        async with asyncio.TaskGroup() as task_group:
            processed_count = 0
            for file_id, file_type in pending_files:
                if self._shutdown_event.is_set():
                    break
                # Launch individual file metadata scraping task
                task_group.create_task(self._scrape_file_metadata_task(file_id))
                processed_count += 1

        return processed_count

    @logfire.instrument("process_pending_image_downloads_batch")
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

        async with asyncio.TaskGroup() as task_group:
            processed_count = 0
            for download in pending_images:
                if self._shutdown_event.is_set():
                    break

                # Launch individual image download task
                task_group.create_task(
                    self._download_image_content_task(
                        download.file_id,
                        download.version,
                        download.filename,
                        download.md5,
                        download.size_bytes,
                        download.download_url,
                    )
                )
                processed_count += 1

        return processed_count

    async def _monitor_queue_depths_periodically(self):
        """Periodically update queue depth gauges for observability."""
        while not self._shutdown_event.is_set():
            try:
                queue_depths = await self.database.get_queue_depths()

                # Update gauges with current queue depths
                self._pending_worlds_gauge.set(queue_depths["pending_worlds"])
                self._pending_files_gauge.set(queue_depths["pending_file_metadata"])
                self._pending_images_gauge.set(queue_depths["pending_image_downloads"])

                # Log queue depths periodically for visibility
                if (
                    queue_depths["pending_worlds"] > 0
                    or queue_depths["pending_file_metadata"] > 0
                    or queue_depths["pending_image_downloads"] > 0
                ):
                    logger.info(
                        f"Queue depths: {queue_depths['pending_worlds']} worlds, "
                        f"{queue_depths['pending_file_metadata']} files, "
                        f"{queue_depths['pending_image_downloads']} images"
                    )

                # Check for shutdown every 30 seconds
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=30.0,
                    )
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    continue

            except Exception as e:
                logger.error(f"Error monitoring queue depths: {e}")
                await self._sleep_func(self._error_backoff_time)

    async def _update_database_with_world(self, world_id: str, world_details_raw: dict):
        """Update database with world metadata and metrics."""
        # Parse raw JSON into WorldDetail only when needed for processing
        world_details = WorldDetail(**world_details_raw)

        # Store the raw API response directly, not the processed metadata
        await self.database.upsert_world_with_files(
            world_id,
            world_details_raw,  # Store raw JSON instead of stable_metadata()
            world_details.extract_metrics(),
            world_details.discovered_files,
            status="SUCCESS",
        )

    @logfire.instrument("scrape_recent_worlds_batch")
    async def _scrape_recent_worlds_batch(self):
        """Execute one round of recent worlds discovery and await completion.

        This is the extracted inner logic from _scrape_recent_worlds_periodically
        without the timing/sleep aspects, useful for testing.
        """
        # Launch recent worlds discovery task and await it
        await self._scrape_recent_worlds_task()

    @logfire.instrument("process_pending_worlds_batch")
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

        async with asyncio.TaskGroup() as task_group:
            processed_count = 0
            for world_id in worlds:
                if self._shutdown_event.is_set():
                    break

                # Launch individual world scraping task
                task_group.create_task(self._scrape_world_task(world_id))
                processed_count += 1

        return processed_count

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
        if hasattr(self.database, "close"):
            await self.database.close()
