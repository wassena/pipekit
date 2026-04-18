"""Tests for pipekit.validators."""
import pytest
from pipekit.validators import (
    validate_fields,
    validate_type,
    validate_range,
    validate_one_of,
)


def test_validate_fields_passes_when_all_present():
    fn = validate_fields(["name", "age"])
    record = {"name": "Alice", "age": 30}
    assert fn(record) == record


def test_validate_fields_raises_on_missing():
    fn = validate_fields(["name", "age"])
    with pytest.raises(ValueError, match="Missing required fields"):
        fn({"name": "Alice"})


def test_validate_fields_does_not_mutate():
    fn = validate_fields(["x"])
    record = {"x": 1, "y": 2}
    result = fn(record)
    assert result is record


def test_validate_type_passes_correct_type():
    fn = validate_type("age", int)
    record = {"age": 25}
    assert fn(record) == record


def test_validate_type_raises_on_wrong_type():
    fn = validate_type("age", int)
    with pytest.raises(TypeError, match="expected int"):
        fn({"age": "twenty"})


def test_validate_type_skips_missing_field():
    fn = validate_type("age", int)
    record = {"name": "Bob"}
    assert fn(record) == record


def test_validate_range_passes_within_range():
    fn = validate_range("score", min_val=0, max_val=100)
    assert fn({"score": 50}) == {"score": 50}


def test_validate_range_raises_below_min():
    fn = validate_range("score", min_val=0)
    with pytest.raises(ValueError, match="below minimum"):
        fn({"score": -1})


def test_validate_range_raises_above_max():
    fn = validate_range("score", max_val=100)
    with pytest.raises(ValueError, match="above maximum"):
        fn({"score": 101})


def test_validate_range_skips_missing_field():
    fn = validate_range("score", min_val=0, max_val=100)
    assert fn({"other": 999}) == {"other": 999}


def test_validate_one_of_passes_valid_choice():
    fn = validate_one_of("status", ["active", "inactive"])
    assert fn({"status": "active"}) == {"status": "active"}


def test_validate_one_of_raises_invalid_choice():
    fn = validate_one_of("status", ["active", "inactive"])
    with pytest.raises(ValueError, match="not in allowed choices"):
        fn({"status": "pending"})


def test_validate_one_of_skips_missing_field():
    fn = validate_one_of("status", ["active"])
    assert fn({"name": "Alice"}) == {"name": "Alice"}


def test_validator_repr_contains_name():
    fn = validate_fields(["x"])
    assert "validate_fields" in fn.__name__
