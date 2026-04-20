"""Tests for pipekit.tap."""

import pytest
from pipekit.tap import tap, tap_each, tap_if


# ---------------------------------------------------------------------------
# tap
# ---------------------------------------------------------------------------

def test_tap_returns_data_unchanged():
    side = []
    step = tap(lambda d: side.append(d))
    result = step([1, 2, 3])
    assert result == [1, 2, 3]


def test_tap_calls_func_once():
    calls = []
    step = tap(lambda d: calls.append(1))
    step("hello")
    assert len(calls) == 1


def test_tap_does_not_mutate_dict():
    original = {"a": 1}
    step = tap(lambda d: d)  # no-op side effect
    result = step(original)
    assert result is original


def test_tap_preserves_func_name():
    def my_logger(data):
        pass

    step = tap(my_logger)
    assert step.__name__ == "my_logger"


def test_tap_works_in_pipeline():
    from pipekit.pipeline import Pipeline

    log = []
    record_step = tap(lambda d: log.append(len(d)))
    pipeline = Pipeline([lambda x: x + [4], record_step])
    result = pipeline([1, 2, 3])
    assert result == [1, 2, 3, 4]
    assert log == [4]


# ---------------------------------------------------------------------------
# tap_each
# ---------------------------------------------------------------------------

def test_tap_each_returns_all_items():
    step = tap_each(lambda x: None)
    result = step([10)
    assert result == [10, 20, 30]


def test_tap_each_calls_func_per_item():
    seen = []
    step = tap_each(seen.append)
    step(["a", "b", "c"])
    assert seen == ["a", "b", "c"]


def test_tap_each_empty_input():
    called = []
    step = tap_each(called.append)
    result = step([])
    assert result == []
    assert called == []


def test_tap_each_does_not_mutate_items():
    items = [{"v": 1}, {"v": 2}]
    step = tap_each(lambda x: x)  # no-op
    result = step(items)
    assert result == [{"v": 1}, {"v": 2}]


# ---------------------------------------------------------------------------
# tap_if
# ---------------------------------------------------------------------------

def test_tap_if_calls_func_when_predicate_true():
    log = []
    step = tap_if(lambda d: len(d) == 0, log.append)
    step([])
    assert len(log) == 1


def test_tap_if_skips_func_when_predicate_false():
    log = []
    step = tap_if(lambda d: len(d) == 0, log.append)
    step([1, 2])
    assert log == []


def test_tap_if_always_returns_data():
    step = tap_if(lambda d: True, lambda d: None)
    data = {"key": "value"}
    assert step(data) is data


def test_tap_if_returns_data_when_predicate_false():
    step = tap_if(lambda d: False, lambda d: None)
    data = [1, 2, 3]
    assert step(data) is data
