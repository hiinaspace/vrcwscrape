import pytest
from vrchat_scraper.rate_limiter import BBRRateLimiter, BbrState
from typing import Any


# A simple helper to make test scenarios more readable
def simulate_request_cycle(
    limiter: BBRRateLimiter,
    req_id: Any,
    time: "MockTime",
    server_latency: float,
    is_success: bool = True,
):
    """Encapsulates sending a request and receiving its response."""
    limiter.on_request_sent(req_id, time.now)
    time.advance(server_latency)
    if is_success:
        limiter.on_success(req_id, time.now)
    else:
        limiter.on_error(req_id, time.now)


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
def mock_time() -> MockTime:
    """Provides a controllable time object for tests."""
    return MockTime()


@pytest.fixture
def limiter(mock_time: MockTime) -> BBRRateLimiter:
    """Provides a BBRRateLimiter instance with predictable test parameters."""
    return BBRRateLimiter(
        now=mock_time.now,
        initial_rate=10.0,
        probe_cycle_duration_sec=10.0,
        min_rate=0.5,  # Add explicit min_rate for consistency
        name="test",
    )


def test_initial_state(limiter: BBRRateLimiter):
    """Test that the limiter initializes with correct default values."""
    assert limiter.state == BbrState.CRUISING
    assert limiter.max_rate == 10.0
    assert limiter._get_effective_rate() == 10.0
    assert limiter.inflight == 0
    assert limiter._short_term_rate_cap == float("inf")


def test_basic_pacing_enforces_rate(limiter: BBRRateLimiter, mock_time: MockTime):
    """Test the core pacing logic of get_delay_until_next_request."""
    # Initial rate is 10.0 req/s, so delay should be 0.1s between requests.

    # First request can go immediately.
    assert limiter.get_delay_until_next_request(mock_time.now) == 0.0
    limiter.on_request_sent("req-1", mock_time.now)

    # Immediately after, we must wait for the full pacing delay.
    assert limiter.get_delay_until_next_request(mock_time.now) == pytest.approx(0.1)

    # Advance time partway.
    mock_time.advance(0.04)
    assert limiter.get_delay_until_next_request(mock_time.now) == pytest.approx(0.06)

    # Advance time past the required delay.
    mock_time.advance(0.06)
    assert limiter.get_delay_until_next_request(mock_time.now) == pytest.approx(0)


def test_delivery_rate_sampling_is_correct(
    limiter: BBRRateLimiter, mock_time: MockTime
):
    """
    Test the core delivery rate sampling logic, ensuring it measures the
    throughput of a flight of requests, not just single request latency.
    """
    # Scenario: Send 3 requests, receive responses staggered.

    # At t=1000, send req-1, req-2, req-3
    limiter.on_request_sent("req-1", mock_time.now)
    mock_time.advance(0.1)  # Stagger sends
    limiter.on_request_sent("req-2", mock_time.now)
    mock_time.advance(0.1)
    limiter.on_request_sent("req-3", mock_time.now)

    # At t=1000.5, response for req-1 arrives.
    mock_time.advance(0.3)
    limiter.on_success("req-1", mock_time.now)
    # total_completed=1, last_response_time=1000.5
    # For req-1: requests_in_sample=1, time_interval=1000.5-1000.0=0.5s
    # delivery_rate = 1 / 0.5 = 2.0 req/s
    assert limiter.max_rate == pytest.approx(2.0)
    assert limiter.min_latency == pytest.approx(0.5)

    # At t=1000.7, response for req-2 arrives.
    mock_time.advance(0.2)
    limiter.on_success("req-2", mock_time.now)
    # total_completed=2, last_response_time=1000.7
    # For req-2: snapshot was (completed=0, last_resp_time=1000.0)
    # requests_in_sample=2, time_interval=1000.7-1000.0=0.7s
    # delivery_rate = 2 / 0.7 = 2.857 req/s
    assert limiter.max_rate == pytest.approx(2.857, rel=1e-3)
    # Latency for req-2 was 1000.7 - 1000.1 = 0.6s. Min latency is still 0.5s.
    assert limiter.min_latency == pytest.approx(0.5)


def test_on_error_applies_short_term_cap(limiter: BBRRateLimiter, mock_time: MockTime):
    """Test that an error applies a temporary brake, not affecting the long-term model."""
    assert limiter.max_rate == 10.0
    assert limiter._short_term_rate_cap == float("inf")

    # Simulate a successful request to establish a baseline.
    simulate_request_cycle(limiter, "req-good", mock_time, 0.1)
    assert limiter.max_rate > 1.0  # Should be updated

    baseline_max_rate = limiter.max_rate

    # Now, simulate an error.
    simulate_request_cycle(limiter, "req-bad", mock_time, 0.1, is_success=False)

    # The long-term max_rate should NOT have changed.
    assert limiter.max_rate == baseline_max_rate
    # But the short-term cap should be applied (10.0 * 0.9 in CRUISING).
    assert limiter._short_term_rate_cap == pytest.approx(baseline_max_rate * 0.9)
    # The effective rate is now lower.
    assert limiter._get_effective_rate() == limiter._short_term_rate_cap


def test_state_machine_cycles_and_updates_gain(
    limiter: BBRRateLimiter, mock_time: MockTime
):
    """Test that the state machine transitions correctly over time."""
    limiter._probe_cycle_duration = 5.0  # Use a short duration for testing

    # Initrate_limiter.ial state
    assert limiter.state == BbrState.CRUISING
    assert limiter._get_pacing_gain() == 1.0

    # Advance time to trigger state change to PROBING_UP
    mock_time.advance(5.1)
    limiter.get_delay_until_next_request(mock_time.now)  # This triggers the update
    assert limiter.state == BbrState.PROBING_UP
    assert limiter._get_pacing_gain() == 1.25

    # Advance time to trigger state change to PROBING_DOWN
    mock_time.advance(5.1)
    limiter.get_delay_until_next_request(mock_time.now)
    assert limiter.state == BbrState.PROBING_DOWN
    assert limiter._get_pacing_gain() == 0.9

    # Advance time to trigger state change back to CRUISING
    mock_time.advance(5.1)
    limiter.get_delay_until_next_request(mock_time.now)
    assert limiter.state == BbrState.CRUISING
    assert limiter._get_pacing_gain() == 1.0


def test_new_probe_cycle_resets_short_term_cap(
    limiter: BBRRateLimiter, mock_time: MockTime
):
    """Test that starting a new probe cycle is optimistic and removes the brake."""
    limiter._probe_cycle_duration = 5.0

    # Induce an error to set the cap.
    simulate_request_cycle(limiter, "req-bad", mock_time, 0.1, is_success=False)
    assert limiter._short_term_rate_cap < float("inf")

    # Advance time into the next cycle.
    mock_time.advance(5.1)
    limiter.get_delay_until_next_request(mock_time.now)  # Trigger state change

    # The cap should be reset, as we are optimistically probing again.
    assert limiter._short_term_rate_cap == float("inf")
    assert limiter._get_effective_rate() == limiter.max_rate


def test_windowed_filter_discards_old_samples(mock_time: MockTime):
    """
    Test that the _WindowedFilter correctly discards samples that have aged
    out of the time window, covering the `while self._history` loop.
    """
    # Use a short window for easy testing
    limiter = BBRRateLimiter(
        now=mock_time.now,
        window_size_sec=5.0,
        initial_rate=100.0,
        min_rate=0.5,  # Explicit min_rate for consistency
        name="test_window",
    )
    # add fake sample for the initial high rate
    limiter._max_rate.update(100.0, mock_time.now)
    assert limiter.max_rate == 100.0

    # At t=1001, add a new, lower sample. The max should still be 100.
    mock_time.advance(1)
    # Manually update to avoid complexity of a full request cycle
    limiter._max_rate.update(10.0, mock_time.now)
    assert limiter.max_rate == 100.0

    # Advance time so the initial high sample (from t=1000) expires.
    # The window is 5s, so at t=1005.1, the sample from t=1000 is gone.
    mock_time.advance(4.1)

    # Add another sample to trigger the update and window cleaning.
    limiter._max_rate.update(5.0, mock_time.now)

    # The max rate should now have dropped to 10.0, as the 100.0 sample
    # was discarded.
    assert limiter.max_rate == 10.0


def test_inflight_limit_triggers_delay(limiter: BBRRateLimiter, mock_time: MockTime):
    """
    Test that exceeding the inflight target imposes a delay, covering
    the `if self.inflight >= max(...)` branch.
    """
    # Set a very low rate to make the inflight target easy to hit.
    limiter._max_rate.update(1.0, mock_time.now)
    limiter._min_latency.update(0.1, mock_time.now)
    # inflight_target will be approx 1.0 * 0.1 = 0.1, but capped by min_pipe_size (4).
    # The limit will be max(4, 4 * 2) = 8.

    # Send 8 requests to hit the limit.
    for i in range(8):
        limiter.on_request_sent(f"req-{i}", mock_time.now)
        mock_time.advance(0.01)  # Avoid all sends at the exact same time

    assert limiter.inflight == 8

    # wait until after the natural pacing should be done
    mock_time.advance(8)

    # The next call should now be delayed, not by pacing, but by the
    # inflight cap. The delay should be based on min_latency.
    delay = limiter.get_delay_until_next_request(mock_time.now)
    assert delay == pytest.approx(limiter.min_latency / 2)
    assert delay == pytest.approx(0.05)


def test_error_during_probing_up_forces_state_to_probing_down(
    limiter: BBRRateLimiter, mock_time: MockTime
):
    """
    Test that receiving an error during the PROBING_UP phase immediately
    transitions the state to PROBING_DOWN.
    """
    limiter._probe_cycle_duration = 5.0  # Use a short duration for testing

    # Advance time to trigger state change to PROBING_UP
    mock_time.advance(5.1)
    limiter.get_delay_until_next_request(mock_time.now)
    assert limiter.state == BbrState.PROBING_UP

    # Now, simulate a failed request while in this state.
    simulate_request_cycle(limiter, "req-probe-fail", mock_time, 0.1, is_success=False)

    # The state should have been immediately forced to PROBING_DOWN,
    # without waiting for the probe cycle timer to elapse.
    assert limiter.state == BbrState.PROBING_DOWN


def test_insufficient_min_rate_still_allows_excessive_delays(mock_time: MockTime):
    """
    Test that when min_rate is set too low, excessive delays can still occur.
    This verifies the min_rate feature works correctly by testing the case where
    min_rate doesn't help because it's lower than the collapsed detected rates.
    """
    # Create a rate limiter with very low min_rate that won't prevent the problem
    limiter = BBRRateLimiter(
        now=mock_time.now,
        initial_rate=5.0,
        min_rate=0.01,  # Too low to prevent excessive delays
        name="test_insufficient_min_rate",
    )

    # Force the max_rate to collapse to a value higher than min_rate
    limiter._max_rate.update(0.1, mock_time.now)  # 0.1 req/s
    limiter._short_term_rate_cap = 0.05  # 0.05 req/s

    # The effective rate should be max(0.01, min(0.1, 0.05)) = max(0.01, 0.05) = 0.05
    # Since min_rate (0.01) < short_term_rate_cap (0.05), min_rate doesn't help
    assert limiter._get_effective_rate() == 0.05

    # Simulate sending a request recently to trigger pacing delay
    limiter.on_request_sent("recent_req", mock_time.now)

    # Check the delay - should still be excessive due to insufficient min_rate
    delay = limiter.get_delay_until_next_request(mock_time.now)

    # With min_rate too low, delay is still 1.0 / 0.05 = 20 seconds
    assert delay == pytest.approx(20.0, rel=1e-3), (
        f"Expected delay of 20.0 seconds with insufficient min_rate, got {delay}"
    )


def test_minimum_rate_prevents_excessive_delays(mock_time: MockTime):
    """
    Test that the minimum rate parameter prevents excessive delays even when
    the detected rate collapses to very low values.
    """
    # Create a rate limiter with a minimum rate of 0.5 req/s (max 2 second delay)
    limiter = BBRRateLimiter(
        now=mock_time.now,
        initial_rate=5.0,
        min_rate=0.5,  # This should prevent delays > 2 seconds
        name="test_min_rate",
    )

    # Force the max_rate to collapse to a very low value
    limiter._max_rate.update(0.1, mock_time.now)  # 0.1 req/s

    # Also set a low short-term rate cap
    limiter._short_term_rate_cap = 0.05  # 0.05 req/s

    # The effective rate should be enforced to the minimum (0.5 req/s)
    # even though max_rate (0.1) and short_term_rate_cap (0.05) are lower
    assert limiter._get_effective_rate() == 0.5

    # Simulate sending a request recently to trigger pacing delay
    limiter.on_request_sent("recent_req", mock_time.now)

    # Check the delay - should be limited by min_rate
    delay = limiter.get_delay_until_next_request(mock_time.now)

    # With min_rate of 0.5 req/s, max delay should be 1.0 / 0.5 = 2.0 seconds
    assert delay <= 2.0, (
        f"Delay should be capped at 2.0 seconds by min_rate, got {delay}"
    )
    assert delay == pytest.approx(2.0, rel=1e-3), (
        f"Expected delay of 2.0 seconds, got {delay}"
    )


def test_min_rate_edge_cases(mock_time: MockTime):
    """
    Test edge cases of min_rate logic to ensure complete branch coverage.
    """
    # Test case 1: min_rate between max_rate and short_term_rate_cap
    limiter = BBRRateLimiter(
        now=mock_time.now,
        initial_rate=5.0,
        min_rate=0.08,  # Between max_rate (0.1) and short_term_rate_cap (0.05)
        name="test_edge_case_1",
    )

    limiter._max_rate.update(0.1, mock_time.now)  # 0.1 req/s
    limiter._short_term_rate_cap = 0.05  # 0.05 req/s (lower than min_rate)

    # Effective rate should be max(0.08, min(0.1, 0.05)) = max(0.08, 0.05) = 0.08
    assert limiter._get_effective_rate() == 0.08

    # Test case 2: min_rate higher than both max_rate and short_term_rate_cap
    limiter2 = BBRRateLimiter(
        now=mock_time.now,
        initial_rate=5.0,
        min_rate=0.2,  # Higher than both detected rates
        name="test_edge_case_2",
    )

    limiter2._max_rate.update(0.1, mock_time.now)  # 0.1 req/s
    limiter2._short_term_rate_cap = 0.15  # 0.15 req/s

    # Effective rate should be max(0.2, min(0.1, 0.15)) = max(0.2, 0.1) = 0.2
    assert limiter2._get_effective_rate() == 0.2

    # Test case 3: Normal operation where detected rates are higher than min_rate
    limiter3 = BBRRateLimiter(
        now=mock_time.now,
        initial_rate=5.0,
        min_rate=0.5,  # Lower than detected rates
        name="test_edge_case_3",
    )

    limiter3._max_rate.update(2.0, mock_time.now)  # 2.0 req/s
    limiter3._short_term_rate_cap = 1.5  # 1.5 req/s

    # Effective rate should be max(0.5, min(2.0, 1.5)) = max(0.5, 1.5) = 1.5
    assert limiter3._get_effective_rate() == 1.5
