import asyncio

import pytest

from notify_flow.llm.resilience import CircuitBreaker, CircuitOpenError, call_with_retries


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


async def nosleep(_):
    return None


def test_breaker_opens_half_opens_and_closes():
    clock = Clock()
    b = CircuitBreaker(failure_threshold=3, reset_s=10, clock=clock)
    for _ in range(3):
        b.record_failure()
    assert b.state == "open" and not b.allow()
    clock.t = 10
    assert b.state == "half_open" and b.allow()
    b.record_failure()  # failed trial reopens immediately
    assert b.state == "open"
    clock.t = 20
    assert b.state == "half_open"
    b.record_success()
    assert b.state == "closed"


async def test_retries_then_succeeds():
    calls = 0

    async def flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("boom")
        return "ok"

    b = CircuitBreaker(5, 30)
    out = await call_with_retries(
        flaky, breaker=b, timeout_s=1, max_attempts=3, backoff_base_s=0.01, sleep=nosleep
    )
    assert out == "ok" and calls == 3 and b.state == "closed"


async def test_timeout_counts_as_failure():
    async def slow():
        await asyncio.sleep(1)

    b = CircuitBreaker(5, 30)
    with pytest.raises(TimeoutError):
        await call_with_retries(
            slow, breaker=b, timeout_s=0.01, max_attempts=2, backoff_base_s=0, sleep=nosleep
        )


async def test_open_breaker_short_circuits_without_calling():
    calls = 0

    async def fn():
        nonlocal calls
        calls += 1
        return "x"

    b = CircuitBreaker(1, 30)
    b.record_failure()
    with pytest.raises(CircuitOpenError):
        await call_with_retries(fn, breaker=b, timeout_s=1, max_attempts=3, backoff_base_s=0)
    assert calls == 0
