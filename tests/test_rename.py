"""Tests for pipekit.rename."""

import pytest

from pipekit.rename import prefix_fields, rename_fields, suffix_fields


# ---------------------------------------------------------------------------
# rename_fields
# ---------------------------------------------------------------------------

def test_rename_fields_renames_single_key():
    records = [{"a": 1, "b": 2}]
    result = rename_fields({"a": "x"})(records)
    assert result == [{"x": 1, "b": 2}]


def test_rename_fields_renames_multiple_keys():
    records = [{"a": 1, "b": 2, "c": 3}]
    result = rename_fields({"a": "x", "b": "y"})(records)
    assert result == [{"x": 1, "y": 2, "c": 3}]


def test_rename_fields_does_not_mutate_original():
    records = [{"a": 1}]
    original = [{"a": 1}]
    rename_fields({"a": "z"})(records)
    assert records == original


def test_rename_fields_skips_missing_field_by_default():
    records = [{"a": 1}]
    result = rename_fields({"missing": "x"})(records)
    assert result == [{"a": 1}]


def test_rename_fields_strict_raises_on_missing_field():
    records = [{"a": 1}]
    with pytest.raises(KeyError, match="missing"):
        rename_fields({"missing": "x"}, strict=True)(records)


def test_rename_fields_empty_input_returns_empty():
    result = rename_fields({"a": "b"})([])
    assert result == []


def test_rename_fields_empty_mapping_returns_unchanged():
    records = [{"a": 1, "b": 2}]
    result = rename_fields({})(records)
    assert result == [{"a": 1, "b": 2}]


def test_rename_fields_applies_to_all_records():
    records = [{"a": 1}, {"a": 2}, {"a": 3}]
    result = rename_fields({"a": "value"})(records)
    assert result == [{"value": 1}, {"value": 2}, {"value": 3}]


# ---------------------------------------------------------------------------
# prefix_fields
# ---------------------------------------------------------------------------

def test_prefix_fields_adds_prefix_to_all_keys():
    records = [{"name": "alice", "age": 30}]
    result = prefix_fields("user_")(records)
    assert result == [{"user_name": "alice", "user_age": 30}]


def test_prefix_fields_respects_exclude():
    records = [{"id": 1, "name": "alice"}]
    result = prefix_fields("raw_", exclude=["id"])(records)
    assert result == [{"id": 1, "raw_name": "alice"}]


def test_prefix_fields_empty_input_returns_empty():
    result = prefix_fields("p_")([])
    assert result == []


def test_prefix_fields_does_not_mutate_original():
    records = [{"a": 1}]
    prefix_fields("p_")(records)
    assert records == [{"a": 1}]


# ---------------------------------------------------------------------------
# suffix_fields
# ---------------------------------------------------------------------------

def test_suffix_fields_adds_suffix_to_all_keys():
    records = [{"score": 0.9, "label": "cat"}]
    result = suffix_fields("_v1")(records)
    assert result == [{"score_v1": 0.9, "label_v1": "cat"}]


def test_suffix_fields_respects_exclude():
    records = [{"id": 7, "score": 0.5}]
    result = suffix_fields("_raw", exclude=["id"])(records)
    assert result == [{"id": 7, "score_raw": 0.5}]


def test_suffix_fields_empty_input_returns_empty():
    result = suffix_fields("_s")([])
    assert result == []


def test_suffix_fields_does_not_mutate_original():
    records = [{"a": 1}]
    suffix_fields("_x")(records)
    assert records == [{"a": 1}]
