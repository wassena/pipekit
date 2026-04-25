import pytest
from pipekit.coalesce import coalesce_field, coalesce_fields


# ---------------------------------------------------------------------------
# coalesce_field
# ---------------------------------------------------------------------------

def test_coalesce_field_leaves_present_value():
    step = coalesce_field("x", 99)
    result = step([{"x": 1}])
    assert result == [{"x": 1}]


def test_coalesce_field_fills_none_with_literal():
    step = coalesce_field("x", 42)
    result = step([{"x": None}])
    assert result == [{"x": 42}]


def test_coalesce_field_fills_from_sibling_field():
    step = coalesce_field("name", "alias")
    records = [{"name": None, "alias": "Bob"}]
    result = step(records)
    assert result[0]["name"] == "Bob"


def test_coalesce_field_fills_from_callable():
    step = coalesce_field("label", lambda r: r["code"].upper())
    records = [{"label": None, "code": "abc"}]
    result = step(records)
    assert result[0]["label"] == "ABC"


def test_coalesce_field_tries_fallbacks_in_order():
    step = coalesce_field("v", "a", "b", 0)
    records = [{"v": None, "a": None, "b": 7}]
    result = step(records)
    assert result[0]["v"] == 7


def test_coalesce_field_uses_last_resort_literal():
    step = coalesce_field("v", "a", 99)
    records = [{"v": None, "a": None}]
    result = step(records)
    assert result[0]["v"] == 99


def test_coalesce_field_custom_null_values():
    step = coalesce_field("score", 0, null_values=[None, "", -1])
    records = [{"score": -1}, {"score": ""}, {"score": 5}]
    result = step(records)
    assert result[0]["score"] == 0
    assert result[1]["score"] == 0
    assert result[2]["score"] == 5


def test_coalesce_field_does_not_mutate_original():
    original = {"x": None}
    step = coalesce_field("x", 10)
    step([original])
    assert original["x"] is None


def test_coalesce_field_empty_input():
    step = coalesce_field("x", 1)
    assert step([]) == []


# ---------------------------------------------------------------------------
# coalesce_fields
# ---------------------------------------------------------------------------

def test_coalesce_fields_fills_multiple():
    step = coalesce_fields(["a", "b"], default=0)
    result = step([{"a": None, "b": None, "c": 3}])
    assert result == [{"a": 0, "b": 0, "c": 3}]


def test_coalesce_fields_leaves_present_values():
    step = coalesce_fields(["a", "b"], default=0)
    result = step([{"a": 1, "b": 2}])
    assert result == [{"a": 1, "b": 2}]


def test_coalesce_fields_default_none_when_unspecified():
    step = coalesce_fields(["x"])
    result = step([{"x": None}])
    assert result[0]["x"] is None


def test_coalesce_fields_does_not_mutate_original():
    original = {"a": None, "b": None}
    step = coalesce_fields(["a", "b"], default="?") 
    step([original])
    assert original["a"] is None
