"""Tests for pipekit.flatten."""

import pytest
from pipekit.flatten import flatten, flatten_field, flatten_records


# ---------------------------------------------------------------------------
# flatten()
# ---------------------------------------------------------------------------

def test_flatten_single_level():
    assert flatten([[1, 2], [3, 4]]) == [1, 2, 3, 4]


def test_flatten_depth_two():
    assert flatten([[1, [2, 3]], [4]], depth=2) == [1, 2, 3, 4]


def test_flatten_unlimited_depth():
    assert flatten([[1, [2, [3, [4]]]]], depth=-1) == [1, 2, 3, 4]


def test_flatten_depth_zero_returns_as_is():
    data = [[1, 2], [3]]
    assert flatten(data, depth=0) == [[1, 2], [3]]


def test_flatten_empty_input():
    assert flatten([]) == []


def test_flatten_no_nesting():
    assert flatten([1, 2, 3]) == [1, 2, 3]


def test_flatten_mixed_types():
    assert flatten([[1, "a"], [True]]) == [1, "a", True]


def test_flatten_does_not_mutate_input():
    data = [[1, 2], [3, [4]]]
    original = [[1, 2], [3, [4]]]
    flatten(data)
    assert data == original


# ---------------------------------------------------------------------------
# flatten_field()
# ---------------------------------------------------------------------------

def test_flatten_field_flattens_list_value():
    records = [{"id": 1, "tags": [["a", "b"], ["c"]]}]
    step = flatten_field("tags")
    result = step(records)
    assert result == [{"id": 1, "tags": ["a", "b", "c"]}]


def test_flatten_field_does_not_mutate_original():
    records = [{"id": 1, "tags": [["a"], ["b"]]}]
    step = flatten_field("tags")
    step(records)
    assert records[0]["tags"] == [["a"], ["b"]]


def test_flatten_field_respects_depth():
    records = [{"x": [[1, [2]], [3]]}]
    step = flatten_field("x", depth=1)
    result = step(records)
    assert result[0]["x"] == [1, [2], 3]


def test_flatten_field_name():
    step = flatten_field("items")
    assert "items" in step.__name__


def test_flatten_field_multiple_records():
    records = [{"v": [[1], [2]]}, {"v": [[3], [4]]}]
    step = flatten_field("v")
    result = step(records)
    assert result == [{"v": [1, 2]}, {"v": [3, 4]}]


# ---------------------------------------------------------------------------
# flatten_records()
# ---------------------------------------------------------------------------

def test_flatten_records_expands_list_field():
    records = [{"id": 1, "tags": ["a", "b"]}]
    step = flatten_records("tags")
    result = step(records)
    assert result == [{"id": 1, "tags": "a"}, {"id": 1, "tags": "b"}]


def test_flatten_records_multiple_source_records():
    records = [{"id": 1, "v": [10, 20]}, {"id": 2, "v": [30]}]
    step = flatten_records("v")
    result = step(records)
    assert result == [
        {"id": 1, "v": 10},
        {"id": 1, "v": 20},
        {"id": 2, "v": 30},
    ]


def test_flatten_records_empty_list_produces_no_rows():
    records = [{"id": 1, "tags": []}]
    step = flatten_records("tags")
    result = step(records)
    assert result == []


def test_flatten_records_does_not_mutate_original():
    records = [{"id": 1, "tags": ["a", "b"]}]
    step = flatten_records("tags")
    step(records)
    assert records[0]["tags"] == ["a", "b"]


def test_flatten_records_with_depth():
    records = [{"id": 1, "v": [[1, 2], [3]]}]
    step = flatten_records("v", depth=1)
    result = step(records)
    assert result == [{"id": 1, "v": 1}, {"id": 1, "v": 2}, {"id": 1, "v": 3}]
