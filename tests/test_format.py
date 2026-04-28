"""Tests for pipekit.format."""

from datetime import date, datetime

import pytest

from pipekit.format import format_date, format_field, format_number


# ---------------------------------------------------------------------------
# format_field
# ---------------------------------------------------------------------------

def test_format_field_combines_fields():
    records = [{"first": "Jane", "last": "Doe"}]
    step = format_field("full_name", "{first} {last}")
    result = step(records)
    assert result[0]["full_name"] == "Jane Doe"


def test_format_field_does_not_mutate_original():
    records = [{"first": "Jane", "last": "Doe"}]
    step = format_field("full_name", "{first} {last}")
    step(records)
    assert "full_name" not in records[0]


def test_format_field_missing_key_uses_default():
    records = [{"first": "Jane"}]
    step = format_field("full_name", "{first} {last}", missing="?")
    result = step(records)
    assert result[0]["full_name"] == "Jane ?"


def test_format_field_overwrites_existing_field():
    records = [{"label": "old", "x": 1, "y": 2}]
    step = format_field("label", "({x}, {y})")
    result = step(records)
    assert result[0]["label"] == "(1, 2)"


def test_format_field_empty_input_returns_empty():
    step = format_field("out", "{a}-{b}")
    assert step([]) == []


def test_format_field_multiple_records():
    records = [{"n": 1}, {"n": 2}, {"n": 3}]
    step = format_field("label", "item-{n}")
    result = step(records)
    assert [r["label"] for r in result] == ["item-1", "item-2", "item-3"]


# ---------------------------------------------------------------------------
# format_number
# ---------------------------------------------------------------------------

def test_format_number_two_decimal_places():
    records = [{"price": 9.5}]
    step = format_number("price", ".2f")
    result = step(records)
    assert result[0]["price"] == "9.50"


def test_format_number_thousands_separator():
    records = [{"count": 1000000}]
    step = format_number("count", ",d")
    result = step(records)
    assert result[0]["count"] == "1,000,000"


def test_format_number_missing_field_left_unchanged():
    records = [{"other": 1}]
    step = format_number("price", ".2f")
    result = step(records)
    assert "price" not in result[0]


def test_format_number_bad_value_uses_on_error():
    records = [{"price": "not-a-number"}]
    step = format_number("price", ".2f", on_error="N/A")
    result = step(records)
    assert result[0]["price"] == "N/A"


def test_format_number_bad_value_no_on_error_leaves_value():
    records = [{"price": "bad"}]
    step = format_number("price", ".2f")
    result = step(records)
    assert result[0]["price"] == "bad"


# ---------------------------------------------------------------------------
# format_date
# ---------------------------------------------------------------------------

def test_format_date_formats_date_object():
    records = [{"created": date(2024, 6, 15)}]
    step = format_date("created", "%Y-%m-%d")
    result = step(records)
    assert result[0]["created"] == "2024-06-15"


def test_format_date_formats_datetime_object():
    records = [{"ts": datetime(2024, 6, 15, 12, 30, 0)}]
    step = format_date("ts", "%d/%m/%Y %H:%M")
    result = step(records)
    assert result[0]["ts"] == "15/06/2024 12:30"


def test_format_date_none_value_left_unchanged():
    records = [{"created": None}]
    step = format_date("created", "%Y-%m-%d")
    result = step(records)
    assert result[0]["created"] is None


def test_format_date_bad_value_uses_on_error():
    records = [{"created": "not-a-date"}]
    step = format_date("created", "%Y-%m-%d", on_error="")
    result = step(records)
    assert result[0]["created"] == ""


def test_format_date_does_not_mutate_original():
    d = date(2024, 1, 1)
    records = [{"created": d}]
    step = format_date("created", "%Y-%m-%d")
    step(records)
    assert records[0]["created"] is d
