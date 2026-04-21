"""Tests for pipekit.window."""

import pytest

from pipekit.window import sliding_window, tumbling_window, window_map


# ---------------------------------------------------------------------------
# sliding_window
# ---------------------------------------------------------------------------

def test_sliding_window_basic():
    result = list(sliding_window([1, 2, 3, 4, 5], size=3))
    assert result == [[1, 2, 3], [2, 3, 4], [3, 4, 5]]


def test_sliding_window_step_two():
    result = list(sliding_window([1, 2, 3, 4, 5], size=3, step=2))
    assert result == [[1, 2, 3], [3, 4, 5]]


def test_sliding_window_size_equals_data():
    result = list(sliding_window([10, 20, 30], size=3))
    assert result == [[10, 20, 30]]


def test_sliding_window_size_larger_than_data():
    result = list(sliding_window([1, 2], size=5))
    assert result == []


def test_sliding_window_empty_input():
    result = list(sliding_window([], size=3))
    assert result == []


def test_sliding_window_size_one():
    result = list(sliding_window([7, 8, 9], size=1))
    assert result == [[7], [8], [9]]


def test_sliding_window_invalid_size():
    with pytest.raises(ValueError, match="size must be"):
        list(sliding_window([1, 2, 3], size=0))


def test_sliding_window_invalid_step():
    with pytest.raises(ValueError, match="step must be"):
        list(sliding_window([1, 2, 3], size=2, step=0))


# ---------------------------------------------------------------------------
# tumbling_window
# ---------------------------------------------------------------------------

def test_tumbling_window_even_split():
    result = list(tumbling_window([1, 2, 3, 4, 5, 6], size=2))
    assert result == [[1, 2], [3, 4], [5, 6]]


def test_tumbling_window_drops_remainder():
    result = list(tumbling_window([1, 2, 3, 4, 5], size=2))
    assert result == [[1, 2], [3, 4]]


def test_tumbling_window_size_larger_than_data():
    result = list(tumbling_window([1, 2], size=5))
    assert result == []


def test_tumbling_window_invalid_size():
    with pytest.raises(ValueError, match="size must be"):
        list(tumbling_window([1, 2, 3], size=-1))


# ---------------------------------------------------------------------------
# window_map
# ---------------------------------------------------------------------------

def test_window_map_sum():
    result = window_map(sum, [1, 2, 3, 4, 5], size=3)
    assert result == [6, 9, 12]


def test_window_map_max():
    result = window_map(max, [3, 1, 4, 1, 5, 9, 2], size=3)
    assert result == [4, 4, 5, 9, 9]


def test_window_map_with_step():
    result = window_map(sum, [1, 2, 3, 4, 5, 6], size=2, step=2)
    assert result == [3, 7, 11]


def test_window_map_returns_list():
    result = window_map(lambda w: w, [1, 2, 3], size=2)
    assert isinstance(result, list)
