import pytest
from vrchat_scraper.circuit_breaker import CircuitBreaker, CircuitBreakerState


class MockTime:
    """A helper class to control time in tests."""

    def __init__(self, start_time: float = 1000.0):
        self._time = start_time

    @property
    def now(self) -> float:
        return self._time

    def advance(self, seconds: float):
        self._time += seconds


@pytest.fixture
def mock_time():
    """Provides a controllable time object for tests."""
    return MockTime()


@pytest.fixture
def breaker():
    """Provides a CircuitBreaker instance with a low threshold for easier testing."""
    return CircuitBreaker(error_threshold=3, initial_backoff_sec=10.0, name="test")


def test_initial_state(breaker: CircuitBreaker):
    """A new circuit breaker should be in the CLOSED state and allow requests."""
    assert breaker.state == CircuitBreakerState.CLOSED
    assert breaker.get_delay_until_proceed(now=0.0) == 0.0
    assert breaker._consecutive_errors == 0


def test_errors_increment_count_but_do_not_trip_below_threshold(
    breaker: CircuitBreaker,
):
    """Test that errors are counted but the state remains CLOSED below the threshold."""
    breaker.on_error(now=1.0)
    assert breaker._consecutive_errors == 1
    assert breaker.state == CircuitBreakerState.CLOSED

    breaker.on_error(now=2.0)
    assert breaker._consecutive_errors == 2
    assert breaker.state == CircuitBreakerState.CLOSED
    assert breaker.get_delay_until_proceed(now=2.0) == 0.0


def test_success_resets_error_count(breaker: CircuitBreaker):
    """A single success should reset the consecutive error count."""
    breaker.on_error(now=1.0)
    breaker.on_error(now=2.0)
    assert breaker._consecutive_errors == 2

    breaker.on_success()
    assert breaker._consecutive_errors == 0
    assert breaker.state == CircuitBreakerState.CLOSED


def test_threshold_reached_trips_breaker_to_open(
    breaker: CircuitBreaker, mock_time: MockTime
):
    """Test the transition from CLOSED to OPEN when the error threshold is met."""
    breaker.on_error(now=mock_time.now)
    mock_time.advance(1)
    breaker.on_error(now=mock_time.now)
    mock_time.advance(1)

    # This is the 3rd error, which should trip the breaker.
    breaker.on_error(now=mock_time.now)

    assert breaker.state == CircuitBreakerState.OPEN
    assert breaker._consecutive_errors == 3

    # Check that it now returns a delay.
    assert breaker.get_delay_until_proceed(now=mock_time.now) == 10.0

    mock_time.advance(4)  # Advance time partway through the backoff
    assert breaker.get_delay_until_proceed(now=mock_time.now) == 6.0


def test_open_state_transitions_to_half_open_after_backoff(
    breaker: CircuitBreaker, mock_time: MockTime
):
    """Test the transition from OPEN to HALF_OPEN after the backoff period."""
    # Trip the breaker
    for _ in range(3):
        breaker.on_error(now=mock_time.now)

    assert breaker.state == CircuitBreakerState.OPEN

    # Advance time to just before the backoff expires
    mock_time.advance(9.9)  # Total elapsed since last error is 9.9
    assert breaker.get_delay_until_proceed(now=mock_time.now) == pytest.approx(0.1)
    assert breaker.state == CircuitBreakerState.OPEN  # Still open

    # Advance time to just after the backoff expires
    mock_time.advance(0.2)  # Total elapsed is 10.1
    assert breaker.get_delay_until_proceed(now=mock_time.now) == 0.0
    assert breaker.state == CircuitBreakerState.HALF_OPEN


def test_half_open_to_closed_on_success(breaker: CircuitBreaker, mock_time: MockTime):
    """Test that a success in HALF_OPEN state resets the breaker."""
    # Manually set the state for this test
    breaker.state = CircuitBreakerState.HALF_OPEN
    breaker._consecutive_errors = 3  # Simulate that it had errors before
    breaker._backoff_duration = 20.0  # Simulate it had a longer backoff

    breaker.on_success()

    assert breaker.state == CircuitBreakerState.CLOSED
    assert breaker._consecutive_errors == 0
    # Crucially, the backoff duration should be reset to its initial value.
    assert breaker._backoff_duration == 10.0
    assert breaker.get_delay_until_proceed(now=mock_time.now) == 0.0


def test_half_open_back_to_open_on_error(breaker: CircuitBreaker, mock_time: MockTime):
    """Test that an error in HALF_OPEN re-trips the breaker with exponential backoff."""
    # Manually set the state
    breaker.state = CircuitBreakerState.HALF_OPEN

    # The first backoff was 10s. The next should be 20s.
    breaker.on_error(now=mock_time.now)

    assert breaker.state == CircuitBreakerState.OPEN
    assert breaker._backoff_duration == 20.0

    # Check that the new, longer delay is in effect
    assert breaker.get_delay_until_proceed(now=mock_time.now) == 20.0


def test_backoff_duration_respects_max_value(mock_time: MockTime):
    """Test that the exponential backoff is capped by max_backoff_sec."""
    # Use a breaker with a low max for easy testing
    breaker = CircuitBreaker(
        error_threshold=1,
        initial_backoff_sec=10.0,
        max_backoff_sec=25.0,
        name="test_backoff",
    )

    # First error -> OPEN, backoff is 10s
    breaker.on_error(now=mock_time.now)
    assert breaker._backoff_duration == 10.0

    # Trigger re-trip (fail in HALF_OPEN)
    mock_time.advance(11)
    breaker.get_delay_until_proceed(now=mock_time.now)  # -> HALF_OPEN
    breaker.on_error(now=mock_time.now)  # -> OPEN again
    assert breaker._backoff_duration == 20.0  # 10 * 2

    # Trigger re-trip again
    mock_time.advance(21)
    breaker.get_delay_until_proceed(now=mock_time.now)  # -> HALF_OPEN
    breaker.on_error(now=mock_time.now)  # -> OPEN again
    # The backoff should be capped at 25, not 40.
    assert breaker._backoff_duration == 25.0
