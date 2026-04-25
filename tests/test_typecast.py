"""Tests for pipekit.typecast."""

import pytest

from pipekit.typecast import CastError, cast_field, cast_fields, cast_step


# ---------------------------------------------------------------------------
# cast_field
# ---------------------------------------------------------------------------

def test_cast_field_converts_string_to_int():
    records = [{"x": "42"}, {"x": "7"}]
    result = cast_field("x", int)(records)
    assert result == [{"x": 42}, {"x": 7}]


def test_cast_field_converts_string_to_float():
    records = [{"v": "3.14"}]
    result = cast_field("v", float)(records)
    assert result[0]["v"] == pytest.approx(3.14)


def test_cast_field_converts_int_to_str():
    records = [{"n": 99}]
    result = cast_field("n", str)(records)
    assert result == [{"n": "99"}]


def test_cast_field_does_not_mutate_original():
    original = [{"x": "1"}]
    cast_field("x", int)(original)
    assert original[0]["x"] == "1"


def test_cast_field_strict_raises_on_bad_value():
    records = [{"x": "not-a-number"}]
    with pytest.raises(CastError):
        cast_field("x", int, strict=True)(records)


def test_cast_field_non_strict_leaves_value_unchanged():
    records = [{"x": "oops"}]
    result = cast_field("x", int, strict=False)(records)
    assert result == [{"x": "oops"}]


def test_cast_field_none_value_stays_none():
    records = [{"x": None}]
    result = cast_field("x", int)(records)
    assert result == [{"x": None}]


def test_cast_field_missing_field_uses_default():
    records = [{"y": 1}]
    result = cast_field("x", int, default=0)(records)
    assert result[0]["x"] == 0


def test_cast_field_preserves_other_fields():
    records = [{"x": "5", "label": "hello"}]
    result = cast_field("x", int)(records)
    assert result[0]["label"] == "hello"


def test_cast_field_empty_input_returns_empty():
    assert cast_field("x", int)([]) == []


# ---------------------------------------------------------------------------
# cast_fields
# ---------------------------------------------------------------------------

def test_cast_fields_converts_multiple_fields():
    records = [{"age": "30", "score": "9.5", "name": "Alice"}]
    result = cast_fields({"age": int, "score": float})(records)
    assert result[0]["age"] == 30
    assert result[0]["score"] == pytest.approx(9.5)
    assert result[0]["name"] == "Alice"


def test_cast_fields_skips_missing_keys():
    records = [{"age": "25"}]
    result = cast_fields({"age": int, "score": float})(records)
    assert "score" not in result[0]


def test_cast_fields_strict_raises():
    records = [{"age": "old"}]
    with pytest.raises(CastError):
        cast_fields({"age": int}, strict=True)(records)


def test_cast_fields_does_not_mutate_original():
    original = [{"x": "1", "y": "2"}]
    cast_fields({"x": int, "y": int})(original)
    assert original[0]["x"] == "1"


# ---------------------------------------------------------------------------
# cast_step alias
# ---------------------------------------------------------------------------

def test_cast_step_is_equivalent_to_cast_field():
    records = [{"n": "10"}]
    assert cast_step("n", int)(records) == cast_field("n", int)(records)
