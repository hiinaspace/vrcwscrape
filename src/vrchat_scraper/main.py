"""Main entry point for VRChat scraper."""

import argparse
import asyncio
import logging
import signal
import subprocess
import time
import sys

import logfire
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import text

from .circuit_breaker import CircuitBreaker
from .config import Config
from .database import Database
from .http_client import AuthenticationError, FileImageDownloader, HTTPVRChatAPIClient
from .rate_limiter import BBRRateLimiter
from .scraper import VRChatScraper

# Configure observability first
logfire.configure(service_name="vrchat-scraper")

# Auto-instrument HTTP and database libraries
HTTPXClientInstrumentor().instrument()
SQLAlchemyInstrumentor().instrument()

# Configure logging with Logfire integration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logfire.LogfireLoggingHandler()],
)
logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Handle graceful shutdown signals."""

    def __init__(self):
        self.shutdown_event = asyncio.Event()

    def handle_signal(self):
        logger.info("Shutdown signal received")
        self.shutdown_event.set()


async def start_dolt_service(db: Database):
    """Start the Dolt SQL server via systemd user service and wait for it to be ready."""
    logger.info("Starting dolt-sql-server service...")
    try:
        subprocess.run(
            ["systemctl", "--user", "start", "dolt-sql-server"],
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("Dolt SQL server service started")

        # Wait for database to be ready
        logger.info("Waiting for database to be ready...")
        max_retries = 30
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                # Try to connect to the database and immediately close
                async with db.engine.connect() as conn:
                    # Simple query to verify connection
                    await conn.execute(text("SELECT 1"))
                logger.info("Database is ready")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.debug(
                        f"Database not ready yet (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(
                        f"Database failed to become ready after {max_retries} attempts"
                    )
                    raise

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start dolt-sql-server service: {e.stderr}")
        raise


async def stop_dolt_service():
    """Stop the Dolt SQL server via systemd user service."""
    logger.info("Stopping dolt-sql-server service...")
    try:
        subprocess.run(
            ["systemctl", "--user", "stop", "dolt-sql-server"],
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("Dolt SQL server service stopped")

        # Wait a moment for the service to fully stop
        await asyncio.sleep(2.0)

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to stop dolt-sql-server service: {e.stderr}")
        # Don't raise - we want to continue cleanup even if stop fails


async def async_main(mode: str = "daemon"):
    """Main application entry point."""
    # Load config
    config = Config()

    # Setup graceful shutdown
    shutdown = GracefulShutdown()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.handle_signal)

    # For oneshot mode, start the Dolt SQL server before database initialization
    # This works around memory leaks by giving each run a fresh server instance
    if mode == "oneshot":
        # Create database object but don't initialize yet
        db = Database(config.database_url)
        await start_dolt_service(db)
    else:
        db = Database(config.database_url)

    try:
        # Initialize database schema
        await db.init_schema()

        # Create API client and image downloader
        api_client = HTTPVRChatAPIClient(config.vrchat_auth_cookie)
        image_downloader = FileImageDownloader(
            config.vrchat_auth_cookie, config.image_storage_path
        )

        # Create rate limiters and circuit breakers
        now = time.time()
        api_rate_limiter = BBRRateLimiter(
            now, initial_rate=10.0, min_rate=0.5, name="api"
        )
        image_rate_limiter = BBRRateLimiter(
            now, initial_rate=20.0, min_rate=0.5, name="image"
        )
        api_circuit_breaker = CircuitBreaker(name="api")
        image_circuit_breaker = CircuitBreaker(name="image")

        # Create main scraper (disable service management since we handle it here)
        scraper = VRChatScraper(
            database=db,
            api_client=api_client,
            image_downloader=image_downloader,
            api_rate_limiter=api_rate_limiter,
            image_rate_limiter=image_rate_limiter,
            api_circuit_breaker=api_circuit_breaker,
            image_circuit_breaker=image_circuit_breaker,
            oneshot_max_worlds=config.oneshot_max_worlds,
            manage_dolt_service=False,  # We manage it here in main instead
        )

        try:
            # Connect shutdown handler to scraper
            def handle_shutdown():
                logger.info("Shutdown signal received")
                scraper.shutdown()
                sys.exit()

            # Override shutdown handler
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, handle_shutdown)

            # Run the scraper in selected mode
            if mode == "daemon":
                await scraper.run_forever()
            elif mode == "oneshot":
                await scraper.run_oneshot()
            else:
                raise ValueError(f"Unknown mode: {mode}")

        except AuthenticationError:
            logger.critical("Authentication failed - exiting")
        except Exception as e:
            logger.error(f"Scraper failed: {e}")
        finally:
            # Cleanup scraper resources
            await scraper.close()
            logger.info("Shutdown complete")

    finally:
        # For oneshot mode, stop the Dolt SQL server to prevent memory leaks
        if mode == "oneshot":
            await stop_dolt_service()


def main():
    parser = argparse.ArgumentParser(description="VRChat world metadata scraper")
    parser.add_argument(
        "--mode",
        choices=["daemon", "oneshot"],
        default="daemon",
        help="Execution mode: daemon (continuous) or oneshot (run once and exit)",
    )

    args = parser.parse_args()
    asyncio.run(async_main(mode=args.mode))


if __name__ == "__main__":
    main()
