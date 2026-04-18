"""Tests for pipekit.retry."""

import pytest
from pipekit.retry import retry


def test_retry_succeeds_on_first_attempt():
    calls = []

    @retry(max_attempts=3)
    def step(x):
        calls.append(x)
        return x * 2

    assert step(5) == 10
    assert len(calls) == 1


def test_retry_succeeds_after_failures():
    attempts = []

    @retry(max_attempts=3, delay=0)
    def flaky(x):
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError("not yet")
        return x + 1

    assert flaky(9) == 10
    assert len(attempts) == 3


def test_retry_raises_after_max_attempts():
    @retry(max_attempts=3, delay=0)
    def always_fails(x):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        always_fails(1)


def test_retry_only_catches_specified_exceptions():
    @retry(max_attempts=3, delay=0, exceptions=(ValueError,))
    def wrong_exc(x):
        raise TypeError("wrong type")

    with pytest.raises(TypeError):
        wrong_exc(1)


def test_retry_on_failure_callback():
    failures = []

    def record(attempt, exc):
        failures.append((attempt, str(exc)))

    @retry(max_attempts=3, delay=0, on_failure=record)
    def failing(x):
        raise ValueError("err")

    with pytest.raises(ValueError):
        failing(0)

    assert len(failures) == 3
    assert failures[0] == (1, "err")
    assert failures[2] == (3, "err")


def test_retry_invalid_max_attempts():
    with pytest.raises(ValueError):
        @retry(max_attempts=0)
        def step(x):
            return x


def test_retry_preserves_function_name():
    @retry(max_attempts=2)
    def my_step(x):
        return x

    assert my_step.__name__ == "my_step"
