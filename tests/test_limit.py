"""Tests for pipekit.limit."""

import pytest

from pipekit.limit import cap_field, drop_while, limit_by, take_while


# ---------------------------------------------------------------------------
# take_while
# ---------------------------------------------------------------------------

def test_take_while_stops_at_first_false():
    step = take_while(lambda r: r["v"] < 5)
    data = [{"v": 1}, {"v": 3}, {"v": 6}, {"v": 2}]
    assert step(data) == [{"v": 1}, {"v": 3}]


def test_take_while_all_pass():
    step = take_while(lambda r: r["v"] > 0)
    data = [{"v": 1}, {"v": 2}, {"v": 3}]
    assert step(data) == data


def test_take_while_none_pass():
    step = take_while(lambda r: r["v"] > 100)
    data = [{"v": 1}, {"v": 2}]
    assert step(data) == []


def test_take_while_empty_input():
    step = take_while(lambda r: True)
    assert step([]) == []


def test_take_while_does_not_mutate():
    original = [{"v": 1}, {"v": 2}]
    step = take_while(lambda r: r["v"] < 10)
    step(original)
    assert original == [{"v": 1}, {"v": 2}]


# ---------------------------------------------------------------------------
# drop_while
# ---------------------------------------------------------------------------

def test_drop_while_skips_leading_records():
    step = drop_while(lambda r: r["v"] < 5)
    data = [{"v": 1}, {"v": 3}, {"v": 6}, {"v": 2}]
    assert step(data) == [{"v": 6}, {"v": 2}]


def test_drop_while_keeps_all_when_first_fails():
    step = drop_while(lambda r: r["v"] > 100)
    data = [{"v": 1}, {"v": 2}]
    assert step(data) == [{"v": 1}, {"v": 2}]


def test_drop_while_drops_all_when_all_pass():
    step = drop_while(lambda r: r["v"] < 100)
    data = [{"v": 1}, {"v": 2}, {"v": 3}]
    assert step(data) == []


def test_drop_while_empty_input():
    step = drop_while(lambda r: True)
    assert step([]) == []


# ---------------------------------------------------------------------------
# limit_by
# ---------------------------------------------------------------------------

def test_limit_by_removes_over_max():
    step = limit_by("age", 30)
    data = [{"age": 25}, {"age": 35}, {"age": 30}]
    assert step(data) == [{"age": 25}, {"age": 30}]


def test_limit_by_passes_through_missing_field():
    step = limit_by("age", 30)
    data = [{"name": "alice"}, {"age": 40}]
    assert step(data) == [{"name": "alice"}]


def test_limit_by_empty_input():
    step = limit_by("score", 100)
    assert step([]) == []


def test_limit_by_does_not_mutate():
    original = [{"age": 20}, {"age": 50}]
    step = limit_by("age", 30)
    step(original)
    assert original == [{"age": 20}, {"age": 50}]


# ---------------------------------------------------------------------------
# cap_field
# ---------------------------------------------------------------------------

def test_cap_field_clamps_over_ceiling():
    step = cap_field("score", 100)
    data = [{"score": 120}, {"score": 80}]
    assert step(data) == [{"score": 100}, {"score": 80}]


def test_cap_field_leaves_equal_value_unchanged():
    step = cap_field("score", 100)
    data = [{"score": 100}]
    assert step(data) == [{"score": 100}]


def test_cap_field_skips_missing_field():
    step = cap_field("score", 100)
    data = [{"name": "alice"}, {"score": 150}]
    result = step(data)
    assert result[0] == {"name": "alice"}
    assert result[1] == {"score": 100}


def test_cap_field_does_not_mutate_original():
    original = [{"score": 200}]
    step = cap_field("score", 100)
    result = step(original)
    assert original[0]["score"] == 200
    assert result[0]["score"] == 100


def test_cap_field_empty_input():
    step = cap_field("score", 100)
    assert step([]) == []
