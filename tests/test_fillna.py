import pytest
from pipekit.fillna import fillna_field, fillna_fields, dropna


# ---------------------------------------------------------------------------
# fillna_field
# ---------------------------------------------------------------------------

def test_fillna_field_fills_none_with_literal():
    records = [{"x": 1, "y": None}, {"x": 2, "y": 3}]
    result = fillna_field("y", 0)(records)
    assert result[0]["y"] == 0
    assert result[1]["y"] == 3


def test_fillna_field_fills_with_callable():
    records = [{"x": 5, "y": None}]
    result = fillna_field("y", lambda r: r["x"] * 2)(records)
    assert result[0]["y"] == 10


def test_fillna_field_does_not_mutate_original():
    records = [{"a": None}]
    original = dict(records[0])
    fillna_field("a", 99)(records)
    assert records[0] == original


def test_fillna_field_only_none_skips_falsy_by_default():
    records = [{"v": 0}, {"v": ""}, {"v": None}]
    result = fillna_field("v", 42)(records)
    assert result[0]["v"] == 0
    assert result[1]["v"] == ""
    assert result[2]["v"] == 42


def test_fillna_field_fills_falsy_when_only_none_false():
    records = [{"v": 0}, {"v": ""}, {"v": None}]
    result = fillna_field("v", 42, only_none=False)(records)
    assert result[0]["v"] == 42
    assert result[1]["v"] == 42
    assert result[2]["v"] == 42


def test_fillna_field_missing_key_treated_as_none():
    records = [{"a": 1}]
    result = fillna_field("b", "default")(records)
    assert result[0]["b"] == "default"


def test_fillna_field_empty_input_returns_empty():
    assert fillna_field("x", 0)([]) == []


def test_fillna_field_has_readable_name():
    step = fillna_field("score", 0)
    assert "score" in step.__name__


# ---------------------------------------------------------------------------
# fillna_fields
# ---------------------------------------------------------------------------

def test_fillna_fields_fills_multiple_fields():
    records = [{"a": None, "b": None, "c": 3}]
    result = fillna_fields({"a": 1, "b": 2})(records)
    assert result[0] == {"a": 1, "b": 2, "c": 3}


def test_fillna_fields_does_not_mutate_original():
    records = [{"a": None}]
    original = dict(records[0])
    fillna_fields({"a": 7})(records)
    assert records[0] == original


def test_fillna_fields_empty_defaults_returns_same_values():
    records = [{"a": 1}]
    result = fillna_fields({})(records)
    assert result == [{"a": 1}]


# ---------------------------------------------------------------------------
# dropna
# ---------------------------------------------------------------------------

def test_dropna_removes_records_with_any_none():
    records = [{"a": 1, "b": 2}, {"a": None, "b": 2}, {"a": 1, "b": None}]
    result = dropna()(records)
    assert result == [{"a": 1, "b": 2}]


def test_dropna_with_specific_fields_ignores_others():
    records = [{"a": None, "b": 2}, {"a": 1, "b": None}]
    result = dropna(fields=["b"])(records)
    assert result == [{"a": None, "b": 2}]


def test_dropna_empty_input_returns_empty():
    assert dropna()([]) == []


def test_dropna_no_nones_returns_all():
    records = [{"a": 1}, {"a": 2}]
    assert dropna()(records) == records
