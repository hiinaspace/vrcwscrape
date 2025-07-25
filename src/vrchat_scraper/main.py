"""Main entry point for VRChat scraper."""

import argparse
import asyncio
import logging
import signal
import time
import sys

import logfire
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

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


async def async_main(mode: str = "daemon"):
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

    # Create API client and image downloader
    api_client = HTTPVRChatAPIClient(config.vrchat_auth_cookie)
    image_downloader = FileImageDownloader(
        config.vrchat_auth_cookie, config.image_storage_path
    )

    # Create rate limiters and circuit breakers
    now = time.time()
    api_rate_limiter = BBRRateLimiter(now, initial_rate=10.0, min_rate=0.5, name="api")
    image_rate_limiter = BBRRateLimiter(
        now, initial_rate=20.0, min_rate=0.5, name="image"
    )
    api_circuit_breaker = CircuitBreaker(name="api")
    image_circuit_breaker = CircuitBreaker(name="image")

    # Create main scraper
    scraper = VRChatScraper(
        database=db,
        api_client=api_client,
        image_downloader=image_downloader,
        api_rate_limiter=api_rate_limiter,
        image_rate_limiter=image_rate_limiter,
        api_circuit_breaker=api_circuit_breaker,
        image_circuit_breaker=image_circuit_breaker,
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
        # Cleanup
        await scraper.close()
        logger.info("Shutdown complete")


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
