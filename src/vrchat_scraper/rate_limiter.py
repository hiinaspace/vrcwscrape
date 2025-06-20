"""BBR-inspired rate limiting for VRChat API."""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RequestResult:
    """Result of a request for rate limiting purposes."""

    timestamp: float
    success: bool
    status_code: Optional[int] = None


class RateLimiter:
    """BBR-inspired rate limiter that adapts to API behavior."""

    def __init__(
        self,
        base_rate: float = 10.0,  # requests per second
        window_size: int = 120,  # seconds
        error_threshold: float = 0.1,
        probe_interval: float = 60.0,
    ):
        """Initialize rate limiter with adaptive parameters."""
        self.base_rate = base_rate
        self.window_size = window_size
        self.error_threshold = error_threshold
        self.probe_interval = probe_interval

        self.request_history = deque()
        self.pacing_gain = 1.0
        self.last_probe_time = time.time()
        self.circuit_breaker_until = 0
        self.backoff_seconds = 5  # Start with 5s backoff
        self.last_request_time = 0

    async def acquire(self):
        """Wait until it's safe to make a request."""
        now = time.time()

        # Check circuit breaker
        if now < self.circuit_breaker_until:
            wait_time = self.circuit_breaker_until - now
            logger.info(f"Circuit breaker active, waiting {wait_time:.1f}s")
            await asyncio.sleep(wait_time)
            now = time.time()

        # Clean old history
        cutoff = now - self.window_size
        while self.request_history and self.request_history[0].timestamp < cutoff:
            self.request_history.popleft()

        # Calculate delay based on current rate
        if self.last_request_time > 0:
            target_rate = self.base_rate * self.pacing_gain
            min_interval = 1.0 / target_rate if target_rate > 0 else 1.0
            elapsed = now - self.last_request_time

            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                await asyncio.sleep(wait_time)

        self.last_request_time = time.time()

    def record_result(self, success: bool, status_code: Optional[int] = None):
        """Record the result of a request and adjust rate accordingly."""
        result = RequestResult(
            timestamp=time.time(),
            success=success,
            status_code=status_code,
        )
        self.request_history.append(result)

        # Calculate error rate
        if len(self.request_history) >= 10:  # Need minimum samples
            recent_errors = sum(
                1
                for r in self.request_history
                if not r.success and r.timestamp > time.time() - 30  # Last 30s
            )
            recent_total = len(
                [r for r in self.request_history if r.timestamp > time.time() - 30]
            )
            error_rate = recent_errors / recent_total if recent_total > 0 else 0

            if error_rate > self.error_threshold:
                self._activate_circuit_breaker()
            elif error_rate > 0:
                # Reduce rate on any errors
                self.pacing_gain = max(0.5, self.pacing_gain * 0.9)
                logger.info(f"Reduced pacing gain to {self.pacing_gain:.2f}")
            else:
                # Consider probing for more bandwidth
                if time.time() - self.last_probe_time > self.probe_interval:
                    self._probe_bandwidth()

        # Reset backoff on success
        if success:
            self.backoff_seconds = 5

    def _activate_circuit_breaker(self):
        """Activate circuit breaker with exponential backoff."""
        self.circuit_breaker_until = time.time() + self.backoff_seconds
        logger.warning(f"Circuit breaker activated for {self.backoff_seconds}s")

        # Exponential backoff for next time
        self.backoff_seconds = min(self.backoff_seconds * 2, 300)  # Max 5 min

        # Reduce rate significantly
        self.pacing_gain = 0.5

    def _probe_bandwidth(self):
        """Temporarily increase rate to test limits."""
        if self.pacing_gain < 0.9:  # Only probe if we're not already near max
            return

        logger.info("Probing for additional bandwidth")
        self.pacing_gain = min(1.2, self.pacing_gain * 1.1)
        self.last_probe_time = time.time()

        # Schedule return to normal after probe
        asyncio.create_task(self._end_probe())

    async def _end_probe(self):
        """End bandwidth probe after test period."""
        await asyncio.sleep(10)  # Probe for 10 seconds

        # Check if probe was successful (no errors in probe period)
        probe_errors = sum(
            1
            for r in self.request_history
            if not r.success and r.timestamp > self.last_probe_time
        )

        if probe_errors == 0:
            logger.info("Probe successful, keeping higher rate")
            self.pacing_gain = min(1.0, self.pacing_gain)  # Don't exceed 1.0
        else:
            logger.info("Probe hit errors, reverting rate")
            self.pacing_gain = 0.9
