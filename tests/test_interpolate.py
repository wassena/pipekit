"""Tests for pipekit.interpolate."""

import pytest

from pipekit.interpolate import (
    interpolate_field,
    interpolate_step,
    _ffill,
    _bfill,
    _linear,
)


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def test_ffill_basic():
    assert _ffill([1, None, None, 4]) == [1, 1, 1, 4]


def test_ffill_leading_none_stays_none():
    assert _ffill([None, None, 3]) == [None, None, 3]


def test_bfill_basic():
    assert _bfill([None, None, 3, None]) == [3, 3, 3, None]


def test_bfill_trailing_none_stays_none():
    assert _bfill([1, None, None]) == [1, None, None]


def test_linear_basic():
    result = _linear([0.0, None, None, 6.0])
    assert result == pytest.approx([0.0, 2.0, 4.0, 6.0])


def test_linear_no_left_anchor():
    result = _linear([None, None, 4.0])
    assert result == [4.0, 4.0, 4.0]


def test_linear_no_right_anchor():
    result = _linear([2.0, None, None])
    assert result == [2.0, 2.0, 2.0]


def test_linear_all_none_unchanged():
    result = _linear([None, None, None])
    assert result == [None, None, None]


# ---------------------------------------------------------------------------
# interpolate_field
# ---------------------------------------------------------------------------

def test_interpolate_field_ffill():
    records = [
        {"t": 1, "v": 10},
        {"t": 2, "v": None},
        {"t": 3, "v": None},
        {"t": 4, "v": 40},
    ]
    step = interpolate_field("v", strategy="ffill")
    result = step(records)
    assert [r["v"] for r in result] == [10, 10, 10, 40]


def test_interpolate_field_bfill():
    records = [
        {"v": None},
        {"v": None},
        {"v": 30},
    ]
    step = interpolate_field("v", strategy="bfill")
    result = step(records)
    assert [r["v"] for r in result] == [30, 30, 30]


def test_interpolate_field_linear():
    records = [{"v": 0.0}, {"v": None}, {"v": None}, {"v": 9.0}]
    step = interpolate_field("v", strategy="linear")
    result = step(records)
    assert [r["v"] for r in result] == pytest.approx([0.0, 3.0, 6.0, 9.0])


def test_interpolate_field_does_not_mutate_original():
    records = [{"v": 1}, {"v": None}]
    original = [{"v": 1}, {"v": None}]
    step = interpolate_field("v", strategy="ffill")
    step(records)
    assert records == original


def test_interpolate_field_preserves_other_keys():
    records = [{"id": 1, "v": 5}, {"id": 2, "v": None}]
    step = interpolate_field("v", strategy="ffill")
    result = step(records)
    assert result[1]["id"] == 2
    assert result[1]["v"] == 5


def test_interpolate_field_missing_key_treated_as_none():
    records = [{"v": 7}, {}]
    step = interpolate_field("v", strategy="ffill")
    result = step(records)
    assert result[1]["v"] == 7


def test_interpolate_field_unknown_strategy_raises():
    with pytest.raises(ValueError, match="Unknown strategy"):
        interpolate_field("v", strategy="spline")


def test_interpolate_field_empty_input():
    step = interpolate_field("v", strategy="ffill")
    assert step([]) == []


# ---------------------------------------------------------------------------
# interpolate_step
# ---------------------------------------------------------------------------

def test_interpolate_step_multiple_fields():
    records = [
        {"a": 1.0, "b": 10.0},
        {"a": None, "b": None},
        {"a": 3.0, "b": 30.0},
    ]
    step = interpolate_step(["a", "b"], strategy="linear")
    result = step(records)
    assert result[1]["a"] == pytest.approx(2.0)
    assert result[1]["b"] == pytest.approx(20.0)


def test_interpolate_step_does_not_mutate_original():
    records = [{"x": 1}, {"x": None}]
    original = [{"x": 1}, {"x": None}]
    step = interpolate_step(["x"], strategy="bfill")
    step(records)
    assert records == original


def test_interpolate_step_func_name():
    step = interpolate_step(["v"], strategy="ffill")
    assert "ffill" in step.__name__
