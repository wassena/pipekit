"""Tests for pipekit.aggregate."""

import pytest
from pipekit.aggregate import aggregate, count_by, group_by


RECORDS = [
    {"category": "a", "value": 10},
    {"category": "b", "value": 20},
    {"category": "a", "value": 30},
    {"category": "b", "value": 5},
    {"category": "c", "value": 7},
]


# ---------------------------------------------------------------------------
# group_by
# ---------------------------------------------------------------------------

def test_group_by_returns_correct_groups():
    groups = group_by("category", RECORDS)
    assert set(groups.keys()) == {"a", "b", "c"}
    assert len(groups["a"]) == 2
    assert len(groups["b"]) == 2
    assert len(groups["c"]) == 1


def test_group_by_empty_input():
    assert group_by("category", []) == {}


def test_group_by_does_not_mutate_records():
    original = [{"k": 1, "v": 2}, {"k": 1, "v": 3}]
    group_by("k", original)
    assert original == [{"k": 1, "v": 2}, {"k": 1, "v": 3}]


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------

def test_aggregate_sum():
    step = aggregate("category", "value", sum)
    result = step(RECORDS)
    by_cat = {r["category"]: r["value"] for r in result}
    assert by_cat["a"] == 40
    assert by_cat["b"] == 25
    assert by_cat["c"] == 7


def test_aggregate_max():
    step = aggregate("category", "value", max)
    result = step(RECORDS)
    by_cat = {r["category"]: r["value"] for r in result}
    assert by_cat["a"] == 30
    assert by_cat["b"] == 20


def test_aggregate_custom_output_field():
    step = aggregate("category", "value", sum, output_field="total")
    result = step(RECORDS)
    assert all("total" in r for r in result)
    assert all("value" not in r for r in result)


def test_aggregate_returns_one_record_per_group():
    step = aggregate("category", "value", sum)
    result = step(RECORDS)
    assert len(result) == 3


def test_aggregate_empty_input():
    step = aggregate("category", "value", sum)
    assert step([]) == []


def test_aggregate_func_name():
    step = aggregate("category", "value", sum)
    assert "category" in step.__name__
    assert "value" in step.__name__


# ---------------------------------------------------------------------------
# count_by
# ---------------------------------------------------------------------------

def test_count_by_returns_correct_counts():
    step = count_by("category")
    result = step(RECORDS)
    by_cat = {r["category"]: r["count"] for r in result}
    assert by_cat["a"] == 2
    assert by_cat["b"] == 2
    assert by_cat["c"] == 1


def test_count_by_custom_output_field():
    step = count_by("category", output_field="n")
    result = step(RECORDS)
    assert all("n" in r for r in result)


def test_count_by_empty_input():
    step = count_by("category")
    assert step([]) == []
