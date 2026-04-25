"""Tests for pipekit.truncate."""

import pytest

from pipekit.truncate import drop, drop_while, slice_records, take, take_while


# ---------------------------------------------------------------------------
# take
# ---------------------------------------------------------------------------

def test_take_returns_first_n():
    assert take(3)([1, 2, 3, 4, 5]) == [1, 2, 3]


def test_take_n_larger_than_input_returns_all():
    assert take(10)([1, 2, 3]) == [1, 2, 3]


def test_take_zero_returns_empty():
    assert take(0)([1, 2, 3]) == []


def test_take_empty_input_returns_empty():
    assert take(5)([]) == []


def test_take_does_not_mutate_original():
    data = [1, 2, 3, 4]
    take(2)(data)
    assert data == [1, 2, 3, 4]


def test_take_negative_raises():
    with pytest.raises(ValueError, match=">= 0"):
        take(-1)


# ---------------------------------------------------------------------------
# drop
# ---------------------------------------------------------------------------

def test_drop_removes_first_n():
    assert drop(2)([1, 2, 3, 4, 5]) == [3, 4, 5]


def test_drop_zero_returns_all():
    assert drop(0)([1, 2, 3]) == [1, 2, 3]


def test_drop_n_larger_than_input_returns_empty():
    assert drop(10)([1, 2, 3]) == []


def test_drop_empty_input_returns_empty():
    assert drop(3)([]) == []


def test_drop_does_not_mutate_original():
    data = [10, 20, 30]
    drop(1)(data)
    assert data == [10, 20, 30]


def test_drop_negative_raises():
    with pytest.raises(ValueError, match=">= 0"):
        drop(-5)


# ---------------------------------------------------------------------------
# slice_records
# ---------------------------------------------------------------------------

def test_slice_records_basic():
    assert slice_records(1, 4)([0, 1, 2, 3, 4, 5]) == [1, 2, 3]


def test_slice_records_with_step():
    assert slice_records(0, 6, 2)([0, 1, 2, 3, 4, 5]) == [0, 2, 4]


def test_slice_records_defaults_return_all():
    assert slice_records()([1, 2, 3]) == [1, 2, 3]


def test_slice_records_zero_step_raises():
    with pytest.raises(ValueError, match="step must not be 0"):
        slice_records(0, 5, 0)


def test_slice_records_empty_input():
    assert slice_records(0, 3)([]) == []


# ---------------------------------------------------------------------------
# take_while
# ---------------------------------------------------------------------------

def test_take_while_stops_at_first_false():
    assert take_while(lambda x: x < 4)([1, 2, 3, 4, 5]) == [1, 2, 3]


def test_take_while_all_match_returns_all():
    assert take_while(lambda x: x > 0)([1, 2, 3]) == [1, 2, 3]


def test_take_while_none_match_returns_empty():
    assert take_while(lambda x: x > 10)([1, 2, 3]) == []


def test_take_while_empty_input():
    assert take_while(lambda x: True)([]) == []


# ---------------------------------------------------------------------------
# drop_while
# ---------------------------------------------------------------------------

def test_drop_while_skips_leading_matches():
    assert drop_while(lambda x: x < 3)([1, 2, 3, 4, 5]) == [3, 4, 5]


def test_drop_while_none_match_returns_all():
    assert drop_while(lambda x: x > 100)([1, 2, 3]) == [1, 2, 3]


def test_drop_while_all_match_returns_empty():
    assert drop_while(lambda x: x < 10)([1, 2, 3]) == []


def test_drop_while_does_not_skip_middle_matches():
    assert drop_while(lambda x: x < 3)([1, 2, 3, 1, 2]) == [3, 1, 2]


def test_drop_while_empty_input():
    assert drop_while(lambda x: True)([]) == []
