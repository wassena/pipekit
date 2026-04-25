"""Tests for pipekit.select."""

import pytest

from pipekit.select import exclude_fields, select_fields, select_if


# ---------------------------------------------------------------------------
# select_fields
# ---------------------------------------------------------------------------

def test_select_fields_keeps_requested_fields():
    records = [{"a": 1, "b": 2, "c": 3}]
    result = select_fields(["a", "c"])(records)
    assert result == [{"a": 1, "c": 3}]


def test_select_fields_multiple_records():
    records = [{"x": 10, "y": 20}, {"x": 30, "y": 40}]
    result = select_fields(["x"])(records)
    assert result == [{"x": 10}, {"x": 30}]


def test_select_fields_silently_skips_missing_by_default():
    records = [{"a": 1}]
    result = select_fields(["a", "z"])(records)
    assert result == [{"a": 1}]


def test_select_fields_strict_raises_on_missing():
    records = [{"a": 1}]
    with pytest.raises(KeyError, match="z"):
        select_fields(["a", "z"], strict=True)(records)


def test_select_fields_does_not_mutate_original():
    record = {"a": 1, "b": 2}
    select_fields(["a"])([record])
    assert record == {"a": 1, "b": 2}


def test_select_fields_empty_input_returns_empty():
    result = select_fields(["a"])([])
    assert result == []


def test_select_fields_empty_field_list_returns_empty_dicts():
    records = [{"a": 1, "b": 2}]
    result = select_fields([])(records)
    assert result == [{}]


# ---------------------------------------------------------------------------
# exclude_fields
# ---------------------------------------------------------------------------

def test_exclude_fields_removes_specified_fields():
    records = [{"a": 1, "b": 2, "c": 3}]
    result = exclude_fields(["b"])(records)
    assert result == [{"a": 1, "c": 3}]


def test_exclude_fields_ignores_absent_fields():
    records = [{"a": 1}]
    result = exclude_fields(["z"])(records)
    assert result == [{"a": 1}]


def test_exclude_fields_does_not_mutate_original():
    record = {"a": 1, "b": 2}
    exclude_fields(["a"])([record])
    assert record == {"a": 1, "b": 2}


def test_exclude_fields_empty_list_leaves_records_unchanged():
    records = [{"a": 1, "b": 2}]
    result = exclude_fields([])(records)
    assert result == [{"a": 1, "b": 2}]


def test_exclude_fields_empty_input_returns_empty():
    result = exclude_fields(["a"])([])
    assert result == []


# ---------------------------------------------------------------------------
# select_if
# ---------------------------------------------------------------------------

def test_select_if_keeps_matching_fields():
    records = [{"a": 1, "b": None, "c": 3}]
    result = select_if(lambda k, v: v is not None)(records)
    assert result == [{"a": 1, "c": 3}]


def test_select_if_filters_by_key_name():
    records = [{"keep_x": 1, "drop_y": 2, "keep_z": 3}]
    result = select_if(lambda k, v: k.startswith("keep"))(records)
    assert result == [{"keep_x": 1, "keep_z": 3}]


def test_select_if_all_fail_returns_empty_dicts():
    records = [{"a": 0, "b": 0}]
    result = select_if(lambda k, v: v > 0)(records)
    assert result == [{}]


def test_select_if_empty_input_returns_empty():
    result = select_if(lambda k, v: True)([])
    assert result == []


def test_select_if_does_not_mutate_original():
    record = {"a": 1, "b": 2}
    select_if(lambda k, v: k == "a")([record])
    assert record == {"a": 1, "b": 2}
