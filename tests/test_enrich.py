"""Tests for pipekit.enrich module."""

import pytest
from pipekit.enrich import enrich_field, enrich_from, enrich_constant


# ---------------------------------------------------------------------------
# enrich_field
# ---------------------------------------------------------------------------

def test_enrich_field_adds_derived_value():
    records = [{"x": 2}, {"x": 5}]
    step = enrich_field("doubled", lambda r: r["x"] * 2)
    result = step(records)
    assert result == [{"x": 2, "doubled": 4}, {"x": 5, "doubled": 10}]


def test_enrich_field_does_not_mutate_original():
    records = [{"x": 1}]
    step = enrich_field("y", lambda r: r["x"] + 10)
    step(records)
    assert "y" not in records[0]


def test_enrich_field_overwrites_existing_by_default():
    records = [{"x": 3, "label": "old"}]
    step = enrich_field("label", lambda r: "new")
    result = step(records)
    assert result[0]["label"] == "new"


def test_enrich_field_no_overwrite_skips_existing():
    records = [{"x": 3, "label": "keep"}, {"x": 4}]
    step = enrich_field("label", lambda r: "new", overwrite=False)
    result = step(records)
    assert result[0]["label"] == "keep"
    assert result[1]["label"] == "new"


def test_enrich_field_empty_input():
    step = enrich_field("y", lambda r: 0)
    assert step([]) == []


def test_enrich_field_has_readable_name():
    step = enrich_field("score", lambda r: 1)
    assert "score" in step.__name__


# ---------------------------------------------------------------------------
# enrich_from
# ---------------------------------------------------------------------------

def test_enrich_from_adds_multiple_fields():
    records = [{"a": 2, "b": 3}]
    step = enrich_from({
        "sum": lambda r: r["a"] + r["b"],
        "product": lambda r: r["a"] * r["b"],
    })
    result = step(records)
    assert result[0]["sum"] == 5
    assert result[0]["product"] == 6


def test_enrich_from_does_not_mutate_original():
    records = [{"a": 1}]
    step = enrich_from({"b": lambda r: 99})
    step(records)
    assert "b" not in records[0]


def test_enrich_from_no_overwrite_preserves_existing():
    records = [{"a": 1, "b": "original"}]
    step = enrich_from({"b": lambda r: "replaced"}, overwrite=False)
    result = step(records)
    assert result[0]["b"] == "original"


def test_enrich_from_empty_mapping():
    records = [{"a": 1}]
    step = enrich_from({})
    result = step(records)
    assert result == [{"a": 1}]


# ---------------------------------------------------------------------------
# enrich_constant
# ---------------------------------------------------------------------------

def test_enrich_constant_sets_same_value_on_all():
    records = [{"x": 1}, {"x": 2}, {"x": 3}]
    step = enrich_constant("source", "pipeline_v1")
    result = step(records)
    assert all(r["source"] == "pipeline_v1" for r in result)


def test_enrich_constant_no_overwrite():
    records = [{"tag": "existing"}, {}]
    step = enrich_constant("tag", "default", overwrite=False)
    result = step(records)
    assert result[0]["tag"] == "existing"
    assert result[1]["tag"] == "default"


def test_enrich_constant_does_not_mutate():
    records = [{"x": 1}]
    step = enrich_constant("flag", True)
    step(records)
    assert "flag" not in records[0]
