"""Tests for pipekit.splitter — split and route utilities."""

import pytest
from pipekit.splitter import split, route


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_even(x):
    return x % 2 == 0


def is_positive(x):
    return x > 0


def is_negative(x):
    return x < 0


def double(x):
    return x * 2


def negate(x):
    return -x


def stringify(x):
    return str(x)


# ---------------------------------------------------------------------------
# split()
# ---------------------------------------------------------------------------

def test_split_basic_partition():
    data = [1, 2, 3, 4, 5, 6]
    evens, odds = split(data, is_even)
    assert evens == [2, 4, 6]
    assert odds == [1, 3, 5]


def test_split_all_match():
    data = [2, 4, 6]
    matched, unmatched = split(data, is_even)
    assert matched == [2, 4, 6]
    assert unmatched == []


def test_split_none_match():
    data = [1, 3, 5]
    matched, unmatched = split(data, is_even)
    assert matched == []
    assert unmatched == [1, 3, 5]


def test_split_empty_input():
    matched, unmatched = split([], is_even)
    assert matched == []
    assert unmatched == []


def test_split_does_not_mutate_input():
    data = [1, 2, 3, 4]
    original = list(data)
    split(data, is_even)
    assert data == original


def test_split_with_dicts():
    data = [
        {"value": 10},
        {"value": -5},
        {"value": 3},
        {"value": -1},
    ]
    pos, neg = split(data, lambda d: d["value"] > 0)
    assert pos == [{"value": 10}, {"value": 3}]
    assert neg == [{"value": -5}, {"value": -1}]


# ---------------------------------------------------------------------------
# route()
# ---------------------------------------------------------------------------

def test_route_applies_correct_branch():
    data = [1, 2, 3, 4, 5]
    result = route(
        data,
        (is_even, double),
        (is_positive, negate),
    )
    # even numbers → doubled; odd positives → negated
    assert result == [negate(1), double(2), negate(3), double(4), negate(5)]


def test_route_first_matching_branch_wins():
    """When multiple predicates match, the first one should be applied."""
    data = [2, 4]  # both is_even and is_positive match
    result = route(
        data,
        (is_even, double),
        (is_positive, negate),
    )
    assert result == [double(2), double(4)]


def test_route_no_matching_branch_returns_item_unchanged():
    data = [-1, -3]  # negative odds — neither branch matches
    result = route(
        data,
        (is_even, double),
        (is_positive, negate),
    )
    assert result == [-1, -3]


def test_route_empty_input():
    result = route([], (is_even, double))
    assert result == []


def test_route_single_branch():
    data = [1, 2, 3]
    result = route(data, (is_even, stringify))
    assert result == [1, "2", 3]


def test_route_does_not_mutate_input():
    data = [1, 2, 3]
    original = list(data)
    route(data, (is_even, double))
    assert data == original


def test_route_no_branches_returns_data_unchanged():
    data = [1, 2, 3]
    result = route(data)
    assert result == [1, 2, 3]
