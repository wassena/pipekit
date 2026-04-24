"""Tests for pipekit.diff."""

import pytest

from pipekit.diff import diff_records, field_diff, diff_step


# ---------------------------------------------------------------------------
# diff_records
# ---------------------------------------------------------------------------

def test_diff_records_detects_added():
    before = [{"id": 1, "v": "a"}]
    after = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    result = diff_records(before, after, key="id")
    assert result["added"] == [{"id": 2, "v": "b"}]
    assert result["removed"] == []
    assert result["changed"] == []


def test_diff_records_detects_removed():
    before = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    after = [{"id": 1, "v": "a"}]
    result = diff_records(before, after, key="id")
    assert result["removed"] == [{"id": 2, "v": "b"}]
    assert result["added"] == []
    assert result["changed"] == []


def test_diff_records_detects_changed():
    before = [{"id": 1, "v": "old"}]
    after = [{"id": 1, "v": "new"}]
    result = diff_records(before, after, key="id")
    assert result["changed"] == [
        {"before": {"id": 1, "v": "old"}, "after": {"id": 1, "v": "new"}}
    ]
    assert result["added"] == []
    assert result["removed"] == []


def test_diff_records_no_changes():
    records = [{"id": i, "v": i * 2} for i in range(5)]
    result = diff_records(records, records[:], key="id")
    assert result == {"added": [], "removed": [], "changed": []}


def test_diff_records_empty_inputs():
    result = diff_records([], [], key="id")
    assert result == {"added": [], "removed": [], "changed": []}


def test_diff_records_missing_key_raises():
    before = [{"id": 1}]
    after = [{"no_id": 1}]  # missing 'id'
    with pytest.raises(KeyError):
        diff_records(before, after, key="id")


# ---------------------------------------------------------------------------
# field_diff
# ---------------------------------------------------------------------------

def test_field_diff_detects_changed_value():
    a = {"x": 1, "y": 2}
    b = {"x": 1, "y": 99}
    result = field_diff(a, b)
    assert result == {"y": {"before": 2, "after": 99}}


def test_field_diff_detects_added_field():
    a = {"x": 1}
    b = {"x": 1, "z": 3}
    result = field_diff(a, b)
    assert result == {"z": {"before": None, "after": 3}}


def test_field_diff_detects_removed_field():
    a = {"x": 1, "z": 3}
    b = {"x": 1}
    result = field_diff(a, b)
    assert result == {"z": {"before": 3, "after": None}}


def test_field_diff_identical_records_empty():
    r = {"a": 1, "b": "hello"}
    assert field_diff(r, r.copy()) == {}


# ---------------------------------------------------------------------------
# diff_step
# ---------------------------------------------------------------------------

def test_diff_step_passes_after_downstream():
    before = [{"id": 1, "v": 10}]
    after = [{"id": 1, "v": 20}]
    step = diff_step(key="id")
    result = step({"before": before, "after": after})
    assert result == after


def test_diff_step_calls_on_diff_callback():
    reports = []
    step = diff_step(key="id", on_diff=reports.append)
    before = [{"id": 1, "v": "a"}]
    after = [{"id": 1, "v": "b"}, {"id": 2, "v": "c"}]
    step({"before": before, "after": after})
    assert len(reports) == 1
    report = reports[0]
    assert len(report["added"]) == 1
    assert len(report["changed"]) == 1


def test_diff_step_no_callback_does_not_raise():
    step = diff_step(key="id")
    result = step({"before": [], "after": [{"id": 1}]})
    assert result == [{"id": 1}]


def test_diff_step_has_descriptive_name():
    step = diff_step(key="user_id")
    assert "user_id" in step.__name__
