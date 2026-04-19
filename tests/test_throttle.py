"""Tests for pipekit.throttle."""

import time
import pytest
from pipekit.throttle import throttle, debounce


def test_throttle_raises_on_zero_rate():
    with pytest.raises(ValueError, match="calls_per_second"):
        throttle(0)


def test_throttle_raises_on_negative_rate():
    with pytest.raises(ValueError):
        throttle(-1.0)


def test_throttle_raises_on_bad_burst():
    with pytest.raises(ValueError, match="burst"):
        throttle(1.0, burst=0)


def test_throttle_allows_burst_calls_immediately():
    counter = {"n": 0}

    @throttle(0.5, burst=3)
    def step(record):
        counter["n"] += 1
        return record

    start = time.monotonic()
    for _ in range(3):
        step({})
    elapsed = time.monotonic() - start
    # Burst of 3 should complete well under 1 second
    assert elapsed < 1.0
    assert counter["n"] == 3


def test_throttle_slows_calls_beyond_burst():
    calls = []

    @throttle(10.0, burst=1)
    def step(record):
        calls.append(time.monotonic())
        return record

    step({})
    step({})
    assert len(calls) == 2
    gap = calls[1] - calls[0]
    # At 10 calls/sec, gap should be ~0.1s
    assert gap >= 0.08


def test_throttle_preserves_return_value():
    @throttle(100.0)
    def double(record):
        return {"v": record["v"] * 2}

    assert double({"v": 5}) == {"v": 10}


def test_throttle_preserves_function_name():
    @throttle(1.0)
    def my_step(record):
        return record

    assert my_step.__name__ == "my_step"


def test_debounce_raises_on_negative_wait():
    with pytest.raises(ValueError):
        debounce(-0.1)


def test_debounce_returns_result():
    @debounce(0.05)
    def step(record):
        return {"done": True, **record}

    result = step({"x": 1})
    assert result == {"done": True, "x": 1}


def test_debounce_preserves_function_name():
    @debounce(0.01)
    def my_step(record):
        return record

    assert my_step.__name__ == "my_step"
