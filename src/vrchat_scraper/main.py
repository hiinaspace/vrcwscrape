"""Main entry point for VRChat scraper."""

import asyncio
import logging
import signal

import logfire

from .config import Config
from .database import Database
from .rate_limiter import RateLimiter
from .scraper import AuthenticationError, Scraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configure observability
logfire.configure()


class GracefulShutdown:
    """Handle graceful shutdown signals."""

    def __init__(self):
        self.shutdown_event = asyncio.Event()

    def handle_signal(self):
        logger.info("Shutdown signal received")
        self.shutdown_event.set()


async def main():
    """Main application entry point."""
    # Load config
    config = Config()

    # Setup graceful shutdown
    shutdown = GracefulShutdown()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.handle_signal)

    # Initialize components
    db = Database(config.database_url)
    await db.init_schema()

    rate_limiter = RateLimiter()
    scraper = Scraper(
        db, rate_limiter, config.vrchat_auth_cookie, config.image_storage_path
    )

    try:
        # Schedule periodic tasks
        async def recent_worlds_task():
            while not shutdown.shutdown_event.is_set():
                try:
                    with logfire.span("scrape_recent_worlds"):
                        await scraper.scrape_recent_worlds()
                except AuthenticationError:
                    logger.critical("Authentication failed - exiting")
                    shutdown.shutdown_event.set()
                    break
                except Exception as e:
                    logger.error(f"Recent worlds scrape failed: {e}")

                # Wait for next run or shutdown
                try:
                    await asyncio.wait_for(
                        shutdown.shutdown_event.wait(),
                        timeout=3600,  # 1 hour
                    )
                except asyncio.TimeoutError:
                    pass  # Continue to next iteration

        async def scrape_pending_task():
            while not shutdown.shutdown_event.is_set():
                try:
                    world_ids = await db.get_worlds_to_scrape(limit=100)

                    if not world_ids:
                        # No worlds to scrape, wait a bit
                        await asyncio.sleep(60)
                        continue

                    for world_id in world_ids:
                        if shutdown.shutdown_event.is_set():
                            break

                        try:
                            with logfire.span("scrape_world", world_id=world_id):
                                await scraper.scrape_world(world_id)
                        except AuthenticationError:
                            logger.critical("Authentication failed - exiting")
                            shutdown.shutdown_event.set()
                            break
                        except Exception as e:
                            logger.error(f"Failed to scrape {world_id}: {e}")

                except Exception as e:
                    logger.error(f"Scraping task error: {e}")
                    await asyncio.sleep(60)  # Back off on errors

        # Run all tasks concurrently
        tasks = [
            asyncio.create_task(recent_worlds_task()),
            asyncio.create_task(scrape_pending_task()),
        ]

        # Wait for shutdown
        await shutdown.shutdown_event.wait()
        logger.info("Shutting down gracefully...")

        # Cancel all tasks
        for task in tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        # Cleanup
        await scraper.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
