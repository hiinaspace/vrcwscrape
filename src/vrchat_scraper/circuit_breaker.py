# vrcwscrape/src/vrchat_scraper/rate_limiter/circuit_breaker.py

from enum import Enum, auto

import logfire


class CircuitBreakerState(Enum):
    """Enumerates the possible states of the CircuitBreaker."""

    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreaker:
    """
    A Circuit Breaker implementation to halt requests during catastrophic failures.

    This pattern provides a critical safety layer on top of a rate limiter. It is
    designed to react to signals that suggest a fundamental problem with making
    requests, such as total API downtime (5xx errors), authentication failure (401),
    or severe, repeated rate limiting (many 429s).
    """

    def __init__(
        self,
        error_threshold: int = 5,
        initial_backoff_sec: float = 5.0,
        max_backoff_sec: float = 300.0,
        name: str = "default",
    ):
        self.state = CircuitBreakerState.CLOSED
        self._error_threshold = error_threshold
        self._initial_backoff_sec = initial_backoff_sec
        self._max_backoff_sec = max_backoff_sec

        self._consecutive_errors = 0
        self._last_error_time = 0.0
        self._backoff_duration = self._initial_backoff_sec

        # --- Observability ---
        self._name = name
        metric_labels = {"instance": name}
        self._state_gauge = logfire.metric_gauge("circuit_breaker.state")
        self._consecutive_errors_gauge = logfire.metric_gauge(
            "circuit_breaker.consecutive_errors"
        )
        self._backoff_duration_gauge = logfire.metric_gauge(
            "circuit_breaker.backoff_duration"
        )
        self._state_transitions_counter = logfire.metric_counter(
            "circuit_breaker.state_transitions"
        )
        self._requests_blocked_counter = logfire.metric_counter(
            "circuit_breaker.requests_blocked"
        )

        # Initialize gauge values
        self._state_gauge.set(self.state.value, metric_labels)
        self._consecutive_errors_gauge.set(self._consecutive_errors, metric_labels)
        self._backoff_duration_gauge.set(self._backoff_duration, metric_labels)

    def _change_state(self, new_state: CircuitBreakerState):
        """Helper method to change state and track metrics."""
        if self.state != new_state:
            self._state_transitions_counter.add(
                1,
                {"from": self.state.name, "to": new_state.name, "instance": self._name},
            )
            self.state = new_state
            self._state_gauge.set(self.state.value, {"instance": self._name})

    def get_delay_until_proceed(self, now: float) -> float:
        """
        Returns the number of seconds to wait before proceeding. 0.0 means
        the action is permitted immediately.
        """
        if self.state == CircuitBreakerState.OPEN:
            # Check if the backoff period has expired.
            if now > self._last_error_time + self._backoff_duration:
                self._change_state(CircuitBreakerState.HALF_OPEN)
                return 0.0  # Permit one trial request

            # If still within the backoff period, return the remaining time.
            delay = (self._last_error_time + self._backoff_duration) - now
            self._requests_blocked_counter.add(1, {"instance": self._name})
            return delay

        # In CLOSED or HALF_OPEN states, we can proceed immediately.
        return 0.0

    def on_success(self):
        """Records a successful outcome, closing the breaker if it was half-open."""
        self._consecutive_errors = 0
        self._consecutive_errors_gauge.set(
            self._consecutive_errors, {"instance": self._name}
        )

        if self.state == CircuitBreakerState.HALF_OPEN:
            self._change_state(CircuitBreakerState.CLOSED)
            self._backoff_duration = self._initial_backoff_sec
            self._backoff_duration_gauge.set(
                self._backoff_duration, {"instance": self._name}
            )

    def on_error(self, now: float):
        """Records a failure, potentially tripping the breaker to the OPEN state."""
        self._consecutive_errors += 1
        self._consecutive_errors_gauge.set(
            self._consecutive_errors, {"instance": self._name}
        )

        # Determine if the breaker should trip based on its current state.
        should_trip = (
            self.state == CircuitBreakerState.HALF_OPEN
            or self._consecutive_errors >= self._error_threshold
        )

        if not should_trip:
            return

        # If we are re-tripping from HALF_OPEN, we double the backoff for the next period.
        if self.state == CircuitBreakerState.HALF_OPEN:
            self._backoff_duration = min(
                self._backoff_duration * 2, self._max_backoff_sec
            )
            self._backoff_duration_gauge.set(
                self._backoff_duration, {"instance": self._name}
            )

        # Now, transition to OPEN state and record the time.
        self._change_state(CircuitBreakerState.OPEN)
        self._last_error_time = now
