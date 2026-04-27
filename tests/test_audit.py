"""Tests for pipekit.audit."""

import pytest

from pipekit.audit import audit_field, audit_step, get_audit_log, strip_audit


# ---------------------------------------------------------------------------
# audit_field
# ---------------------------------------------------------------------------

def test_audit_field_appends_entry():
    records = [{"name": "alice", "score": 10}]
    result = audit_field("score")(records)
    log = get_audit_log(result[0])
    assert len(log) == 1
    assert log[0]["field"] == "score"
    assert log[0]["value"] == 10


def test_audit_field_uses_custom_label():
    records = [{"score": 7}]
    result = audit_field("score", label="raw_score")(records)
    assert get_audit_log(result[0])[0]["field"] == "raw_score"


def test_audit_field_missing_field_stores_none():
    records = [{"name": "bob"}]
    result = audit_field("score")(records)
    assert get_audit_log(result[0])[0]["value"] is None


def test_audit_field_does_not_mutate_original():
    original = {"score": 5}
    records = [original]
    audit_field("score")(records)
    assert "_audit" not in original


def test_audit_field_accumulates_across_calls():
    records = [{"a": 1, "b": 2}]
    step1 = audit_field("a")
    step2 = audit_field("b")
    result = step2(step1(records))
    log = get_audit_log(result[0])
    assert len(log) == 2
    assert log[0]["field"] == "a"
    assert log[1]["field"] == "b"


def test_audit_field_includes_timestamp_when_requested():
    records = [{"x": 99}]
    result = audit_field("x", include_timestamp=True)(records)
    entry = get_audit_log(result[0])[0]
    assert "ts" in entry
    assert isinstance(entry["ts"], float)


# ---------------------------------------------------------------------------
# audit_step
# ---------------------------------------------------------------------------

def test_audit_step_snapshots_all_fields():
    records = [{"a": 1, "b": 2}]
    result = audit_step("after_load")(records)
    log = get_audit_log(result[0])
    assert log[0]["step"] == "after_load"
    assert log[0]["snapshot"] == {"a": 1, "b": 2}


def test_audit_step_snapshots_subset_of_fields():
    records = [{"a": 1, "b": 2, "c": 3}]
    result = audit_step("check", fields=["a", "c"])(records)
    assert get_audit_log(result[0])[0]["snapshot"] == {"a": 1, "c": 3}


def test_audit_step_skips_missing_fields_silently():
    records = [{"a": 1}]
    result = audit_step("check", fields=["a", "z"])(records)
    assert get_audit_log(result[0])[0]["snapshot"] == {"a": 1}


def test_audit_step_does_not_include_audit_in_snapshot():
    records = [{"x": 5}]
    step = audit_step("s1")
    result = step(step(records))  # two snapshots
    snapshots = [e["snapshot"] for e in get_audit_log(result[0])]
    for snap in snapshots:
        assert "_audit" not in snap


def test_audit_step_includes_timestamp_when_requested():
    records = [{"v": 0}]
    result = audit_step("ts_step", include_timestamp=True)(records)
    assert "ts" in get_audit_log(result[0])[0]


# ---------------------------------------------------------------------------
# strip_audit
# ---------------------------------------------------------------------------

def test_strip_audit_removes_key():
    records = [{"a": 1, "_audit": [{"field": "a", "value": 1}]}]
    result = strip_audit(records)
    assert "_audit" not in result[0]
    assert result[0] == {"a": 1}


def test_strip_audit_does_not_mutate_original():
    original = {"a": 1, "_audit": []}
    strip_audit([original])
    assert "_audit" in original


def test_strip_audit_empty_input():
    assert strip_audit([]) == []


def test_get_audit_log_returns_empty_list_when_absent():
    assert get_audit_log({"x": 1}) == []
