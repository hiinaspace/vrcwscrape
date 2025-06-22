from collections import deque, namedtuple
from enum import Enum, auto
from typing import Deque, Tuple, Dict, Any, Callable


class BbrState(Enum):
    """Enumerates the probing states of the BBR-inspired rate limiter."""

    CRUISING = auto()
    PROBING_UP = auto()
    PROBING_DOWN = auto()


# A lightweight struct to hold the limiter's state when a request was sent.
RequestState = namedtuple(
    "RequestState", ["send_time", "completed_at_send", "last_response_time_at_send"]
)


class _WindowedFilter:
    """
    An efficient implementation for tracking the min/max of values within a
    sliding time window using the "monotonic deque" algorithm.

    This ensures that each update operation is amortized O(1), as each element
    is added to and removed from the internal deque only once.
    """

    def __init__(
        self,
        window_size: float,
        initial_value: float,
        op: Callable,
        op_is_better: Callable,
    ):
        self._window_size = window_size
        self._op = op  # The operator (min or max)
        self._op_is_better = op_is_better  # The comparison (e.g., < for min, > for max)
        self._history: Deque[Tuple[float, float]] = deque()
        self.value = initial_value

    def update(self, new_sample: float, now: float):
        # Remove old samples from the front of the deque.
        cutoff = now - self._window_size
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()

        # Remove elements from the back that are "worse" than the new sample.
        # For a min-filter, we remove larger elements.
        # For a max-filter, we remove smaller elements.
        while self._history and self._op_is_better(new_sample, self._history[-1][1]):
            self._history.pop()

        self._history.append((now, new_sample))

        # The best value in the window is always at the front of the deque.
        self.value = self._history[0][1]


class BBRRateLimiter:
    """
    Adapts API request rate using an algorithm inspired by BBR congestion control.

    This implementation maintains a model of the API's capacity by measuring request
    latency and delivery rate. It does not handle catastrophic failures (that is the
    job of the CircuitBreaker); instead, it focuses on finding the optimal
    performance point and reacting gracefully to transient congestion (e.g., a few 429s).

    BBR Terminology Mapping:
    The design is justified by concepts in the BBR spec (draft-ietf-ccwg-bbr-02).
    - BBR.max_bw (Long-term model): Our `_max_rate` windowed-max filter. It stores
      the optimistic, best-observed delivery rate.
    - BBR.bw_shortterm (Short-term model): Our `_short_term_rate_cap`. This is a
      pessimistic "brake" applied immediately on errors.
    - BBR.bw (Effective Bandwidth): The `min(self.max_rate, self._short_term_rate_cap)`.
    - Delivery Rate Sampling (BBR Spec 4.5.2): The core logic in `on_success` that
      calculates throughput based on a flight of requests, not just one.
    - ProbeBW Cycle (BBR Spec 4.3.3): Our state machine (`CRUISING`, `PROBING_UP`,
      `PROBING_DOWN`) mimics this to safely probe for more capacity.

    Note on App-Limited Detection:
    Unlike TCP BBR which operates across 12 orders of magnitude, this rate limiter
    operates at 1-10 RPS with short windows (10s). App-limited scenarios (where we
    have insufficient work to saturate the rate limit) will temporarily lower the
    detected rate, but the probe cycle quickly recovers. The complexity of proper
    app-limited detection isn't justified at our scale and steady-state operation.
    """

    def __init__(
        self,
        now: float,
        initial_rate: float = 2.0,
        probe_cycle_duration_sec: float = 5.0,
        window_size_sec: float = 10.0,
        min_requests_for_pipe: int = 4,
        probe_up_gain: float = 1.25,
        probe_down_gain: float = 0.9,
    ):
        # --- BBR State & Model ---
        self.state = BbrState.CRUISING
        self._max_rate = _WindowedFilter(
            window_size_sec, initial_rate, max, lambda a, b: a >= b
        )
        self._short_term_rate_cap = float("inf")
        self._min_latency = _WindowedFilter(
            window_size_sec, 0.5, min, lambda a, b: a <= b
        )

        # --- Delivery Rate Sampling State ---
        self._total_requests_completed = 0
        self._last_response_time = now

        # --- Concurrency & Pacing ---
        self.inflight = 0
        self._pacing_rate = initial_rate
        self._inflight_target = initial_rate * self._min_latency.value
        self._min_pipe_size = min_requests_for_pipe
        self._probe_up_gain = probe_up_gain
        self._probe_down_gain = probe_down_gain

        # --- In-flight Request Tracking ---
        self._inflight_requests: Dict[Any, RequestState] = {}

        # --- Timing ---
        self._last_send_time = 0.0
        self._last_probe_cycle_start = now
        self._probe_cycle_duration = probe_cycle_duration_sec

    @property
    def max_rate(self) -> float:
        """Returns the long-term, windowed-max delivery rate estimate."""
        return self._max_rate.value

    @property
    def min_latency(self) -> float:
        """Returns the windowed-minimum latency estimate."""
        return self._min_latency.value

    def get_delay_until_next_request(self, now: float) -> float:
        """
        Returns the seconds to wait before sending the next request. 0.0 means
        the request can be sent immediately.
        """
        self._update_state_machine(now)

        time_since_last = now - self._last_send_time
        pacing_rate = self._get_effective_rate() * self._get_pacing_gain()
        required_delay = 1.0 / pacing_rate if pacing_rate > 0 else float("inf")

        if time_since_last < required_delay:
            return required_delay - time_since_last

        inflight_target = max(
            self._min_pipe_size, self._get_effective_rate() * self.min_latency
        )
        if self.inflight >= max(self._min_pipe_size, inflight_target * 2):
            return self.min_latency / 2

        return 0.0

    def on_request_sent(self, request_id: Any, now: float):
        """Call this immediately after dispatching a request."""
        self.inflight += 1
        self._last_send_time = now
        self._inflight_requests[request_id] = RequestState(
            send_time=now,
            completed_at_send=self._total_requests_completed,
            last_response_time_at_send=self._last_response_time,
        )

    def on_success(self, request_id: Any, now: float):
        """Records a successful request, updating the rate model."""
        request_state = self._inflight_requests.pop(request_id, None)
        if not request_state:
            return

        self.inflight = max(0, self.inflight - 1)
        self._total_requests_completed += 1

        requests_in_sample = (
            self._total_requests_completed - request_state.completed_at_send
        )
        time_interval = now - request_state.last_response_time_at_send
        self._last_response_time = now

        if requests_in_sample > 0 and time_interval > 0:
            delivery_rate = requests_in_sample / time_interval
            self._max_rate.update(delivery_rate, now)

        latency = now - request_state.send_time
        self._min_latency.update(latency, now)

    def on_error(self, request_id: Any, now: float):
        """
        Records a failed request, applying a short-term brake on the rate.
        This signals transient congestion.
        """
        if request_id in self._inflight_requests:
            self._inflight_requests.pop(request_id)
            self.inflight = max(0, self.inflight - 1)

        effective_rate = self._get_effective_rate()

        # Apply the pessimistic short-term brake.
        factor = 0.8 if self.state == BbrState.PROBING_UP else 0.9
        self._short_term_rate_cap = min(
            self._short_term_rate_cap, effective_rate * factor
        )

        # A probe that results in an error has failed; immediately back off.
        if self.state == BbrState.PROBING_UP:
            self.state = BbrState.PROBING_DOWN

    def _get_effective_rate(self) -> float:
        return min(self.max_rate, self._short_term_rate_cap)

    def _get_pacing_gain(self) -> float:
        return {
            BbrState.CRUISING: 1.0,
            BbrState.PROBING_UP: self._probe_up_gain,
            BbrState.PROBING_DOWN: self._probe_down_gain,
        }[self.state]

    def _update_state_machine(self, now: float):
        if now > self._last_probe_cycle_start + self._probe_cycle_duration:
            self._last_probe_cycle_start = now
            # A new probe cycle is a chance to be optimistic again. Remove the brake.
            self._short_term_rate_cap = float("inf")

            if self.state == BbrState.CRUISING:
                self.state = BbrState.PROBING_UP
            elif self.state == BbrState.PROBING_UP:
                self.state = BbrState.PROBING_DOWN
            elif self.state == BbrState.PROBING_DOWN:
                self.state = BbrState.CRUISING
