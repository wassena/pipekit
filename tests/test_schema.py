"""Tests for pipekit.schema."""

import pytest
from pipekit.schema import apply_schema, schema_step, SchemaError


# ---------------------------------------------------------------------------
# apply_schema
# ---------------------------------------------------------------------------

def test_apply_schema_passes_valid_record():
    step = apply_schema({"name": str, "age": int})
    record = {"name": "Alice", "age": 30}
    assert step(record) == record


def test_apply_schema_does_not_mutate_original():
    step = apply_schema({"x": int}, coerce=True)
    original = {"x": "42"}
    result = step(original)
    assert original["x"] == "42"
    assert result["x"] == 42


def test_apply_schema_raises_on_missing_field():
    step = apply_schema({"name": str, "age": int})
    with pytest.raises(SchemaError, match="Missing required field"):
        step({"name": "Bob"})


def test_apply_schema_raises_on_wrong_type():
    step = apply_schema({"count": int})
    with pytest.raises(SchemaError, match="expected int"):
        step({"count": "not-a-number"})


def test_apply_schema_coerces_value():
    step = apply_schema({"count": int}, coerce=True)
    result = step({"count": "7"})
    assert result["count"] == 7
    assert isinstance(result["count"], int)


def test_apply_schema_coerce_failure_raises():
    step = apply_schema({"count": int}, coerce=True)
    with pytest.raises(SchemaError, match="Cannot coerce"):
        step({"count": "abc"})


def test_apply_schema_allows_extra_fields_by_default():
    step = apply_schema({"x": int})
    result = step({"x": 1, "y": 2})
    assert result["y"] == 2


def test_apply_schema_rejects_extra_fields_when_disallowed():
    step = apply_schema({"x": int}, allow_extra=False)
    with pytest.raises(SchemaError, match="Unexpected fields"):
        step({"x": 1, "y": 2})


def test_apply_schema_preserves_unvalidated_fields():
    step = apply_schema({"x": int})
    result = step({"x": 1, "meta": "info"})
    assert result["meta"] == "info"


# ---------------------------------------------------------------------------
# schema_step
# ---------------------------------------------------------------------------

def test_schema_step_validates_all_records():
    step = schema_step({"id": int, "label": str})
    records = [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]
    result = step(records)
    assert result == records


def test_schema_step_raises_on_invalid_record():
    step = schema_step({"id": int})
    with pytest.raises(SchemaError):
        step([{"id": 1}, {"label": "oops"}])


def test_schema_step_coerces_all_records():
    step = schema_step({"value": float}, coerce=True)
    result = step([{"value": "1.5"}, {"value": "2.5"}])
    assert result == [{"value": 1.5}, {"value": 2.5}]


def test_schema_step_empty_input():
    step = schema_step({"x": int})
    assert step([]) == []
