"""Tests for pipekit.transforms built-in transform functions."""

import pytest
from pipekit.transforms import (
    map_field,
    filter_field,
    rename_field,
    drop_fields,
    apply_to_each,
    add_field,
)


def test_map_field_applies_func():
    record = {"name": "alice", "age": 30}
    result = map_field("name", str.upper)(record)
    assert result == {"name": "ALICE", "age": 30}


def test_map_field_does_not_mutate():
    record = {"val": 5}
    map_field("val", lambda x: x * 2)(record)
    assert record["val"] == 5


def test_filter_field_keeps_matching():
    data = [{"x": 1}, {"x": 2}, {"x": 3}]
    result = filter_field("x", lambda v: v > 1)(data)
    assert result == [{"x": 2}, {"x": 3}]


def test_filter_field_empty_result():
    data = [{"x": 1}]
    result = filter_field("x", lambda v: v > 10)(data)
    assert result == []


def test_rename_field():
    record = {"old_key": 42}
    result = rename_field("old_key", "new_key")(record)
    assert result == {"new_key": 42}
    assert "old_key" not in result


def test_drop_fields_removes_keys():
    record = {"a": 1, "b": 2, "c": 3}
    result = drop_fields("b", "c")(record)
    assert result == {"a": 1}


def test_drop_fields_missing_key_ignored():
    record = {"a": 1}
    result = drop_fields("b")(record)
    assert result == {"a": 1}


def test_apply_to_each():
    result = apply_to_each(lambda x: x * 2)([1, 2, 3])
    assert result == [2, 4, 6]


def test_apply_to_each_empty():
    result = apply_to_each(str)([])
    assert result == []


def test_add_field():
    record = {"price": 10, "qty": 3}
    result = add_field("total", lambda d: d["price"] * d["qty"])(record)
    assert result["total"] == 30
    assert result["price"] == 10


def test_transform_names_are_descriptive():
    assert "age" in map_field("age", int).__name__
    assert "name" in filter_field("name", bool).__name__
    assert "drop_fields" in drop_fields("x", "y").__name__
