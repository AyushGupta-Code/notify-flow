"""Timeouts, retries with exponential backoff and jitter, and a circuit breaker."""

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from notify_flow.metrics import BREAKER_STATE

T = TypeVar("T")


class CircuitOpenError(Exception):
    pass


class CircuitBreaker:
    """Classic three-state breaker. Opens after `failure_threshold` consecutive failures,
    lets one trial call through after `reset_s`, closes again on success."""

    CLOSED, HALF_OPEN, OPEN = "closed", "half_open", "open"
    _GAUGE = {CLOSED: 0, HALF_OPEN: 1, OPEN: 2}

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_s: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.reset_s = reset_s
        self._clock = clock
        self._failures = 0
        self._opened_at: float | None = None
        self._state = self.CLOSED
        BREAKER_STATE.set(0)

    @property
    def state(self) -> str:
        if self._state == self.OPEN and self._opened_at is not None:
            if self._clock() - self._opened_at >= self.reset_s:
                self._set(self.HALF_OPEN)
        return self._state

    def _set(self, state: str) -> None:
        self._state = state
        BREAKER_STATE.set(self._GAUGE[state])

    def allow(self) -> bool:
        return self.state != self.OPEN

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None
        self._set(self.CLOSED)

    def record_failure(self) -> None:
        self._failures += 1
        if self._state == self.HALF_OPEN or self._failures >= self.failure_threshold:
            self._opened_at = self._clock()
            self._set(self.OPEN)


async def call_with_retries(
    fn: Callable[[], Awaitable[T]],
    *,
    breaker: CircuitBreaker,
    timeout_s: float,
    max_attempts: int,
    backoff_base_s: float,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    last: BaseException | None = None
    for attempt in range(max_attempts):
        if not breaker.allow():
            raise CircuitOpenError("circuit open") from last
        try:
            result = await asyncio.wait_for(fn(), timeout=timeout_s)
        except retry_on as exc:  # includes TimeoutError
            last = exc
            breaker.record_failure()
            if attempt + 1 < max_attempts:
                delay = backoff_base_s * (2**attempt)
                await sleep(delay + random.uniform(0, delay / 2))
            continue
        breaker.record_success()
        return result
    assert last is not None
    raise last
