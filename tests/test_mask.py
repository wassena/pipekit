"""Tests for pipekit.mask."""

import pytest

from pipekit.mask import drop_fields, mask_field, redact_pattern


# ---------------------------------------------------------------------------
# mask_field
# ---------------------------------------------------------------------------

def test_mask_field_replaces_value():
    records = [{"name": "Alice", "ssn": "123-45-6789"}]
    result = mask_field("ssn")(records)
    assert result[0]["ssn"] == "***"
    assert result[0]["name"] == "Alice"


def test_mask_field_custom_mask():
    records = [{"token": "abc123"}]
    result = mask_field("token", mask_with="HIDDEN")(records)
    assert result[0]["token"] == "HIDDEN"


def test_mask_field_partial_exposes_tail():
    records = [{"card": "4111111111111234"}]
    result = mask_field("card", mask_with="****", partial=True, visible_chars=4)(records)
    assert result[0]["card"] == "****1234"


def test_mask_field_partial_short_value_fully_masked():
    records = [{"pin": "42"}]
    result = mask_field("pin", mask_with="***", partial=True, visible_chars=4)(records)
    assert result[0]["pin"] == "***"


def test_mask_field_missing_field_left_unchanged():
    records = [{"name": "Bob"}]
    result = mask_field("ssn")(records)
    assert result[0] == {"name": "Bob"}


def test_mask_field_none_value_left_unchanged():
    records = [{"ssn": None}]
    result = mask_field("ssn")(records)
    assert result[0]["ssn"] is None


def test_mask_field_does_not_mutate_original():
    original = {"ssn": "123-45-6789"}
    records = [original]
    mask_field("ssn")(records)
    assert original["ssn"] == "123-45-6789"


def test_mask_field_empty_input():
    assert mask_field("ssn")([]) == []


# ---------------------------------------------------------------------------
# redact_pattern
# ---------------------------------------------------------------------------

def test_redact_pattern_replaces_match():
    records = [{"note": "Call me at 555-867-5309 please."}]
    result = redact_pattern("note", r"\d{3}-\d{3}-\d{4}")(records)
    assert result[0]["note"] == "Call me at [REDACTED] please."


def test_redact_pattern_custom_replacement():
    records = [{"email": "user@example.com"}]
    result = redact_pattern("email", r"[^@]+@[^@]+", replacement="<email>")(records)
    assert result[0]["email"] == "<email>"


def test_redact_pattern_no_match_unchanged():
    records = [{"text": "nothing sensitive here"}]
    result = redact_pattern("text", r"\d{16}")(records)
    assert result[0]["text"] == "nothing sensitive here"


def test_redact_pattern_non_string_field_skipped():
    records = [{"value": 12345}]
    result = redact_pattern("value", r"\d+")(records)
    assert result[0]["value"] == 12345


def test_redact_pattern_does_not_mutate_original():
    original = {"note": "SSN: 123-45-6789"}
    records = [original]
    redact_pattern("note", r"\d{3}-\d{2}-\d{4}")(records)
    assert original["note"] == "SSN: 123-45-6789"


# ---------------------------------------------------------------------------
# drop_fields
# ---------------------------------------------------------------------------

def test_drop_fields_removes_specified_fields():
    records = [{"a": 1, "b": 2, "c": 3}]
    result = drop_fields("b", "c")(records)
    assert result == [{"a": 1}]


def test_drop_fields_missing_field_is_safe():
    records = [{"a": 1}]
    result = drop_fields("z")(records)
    assert result == [{"a": 1}]


def test_drop_fields_does_not_mutate_original():
    original = {"a": 1, "secret": "x"}
    drop_fields("secret")([original])
    assert "secret" in original


def test_drop_fields_empty_input():
    assert drop_fields("a")([]) == []
