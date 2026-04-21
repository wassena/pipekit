import pytest
from pipekit.dedupe import dedupe, dedupe_field


# ---------------------------------------------------------------------------
# dedupe — hashable records
# ---------------------------------------------------------------------------

def test_dedupe_removes_duplicates_keep_first():
    step = dedupe()
    result = step([1, 2, 3, 2, 1, 4])
    assert result == [1, 2, 3, 4]


def test_dedupe_removes_duplicates_keep_last():
    step = dedupe(keep="last")
    result = step([1, 2, 3, 2, 1, 4])
    assert result == [3, 2, 1, 4]


def test_dedupe_empty_input():
    step = dedupe()
    assert step([]) == []


def test_dedupe_no_duplicates_unchanged():
    step = dedupe()
    data = [10, 20, 30]
    assert step(data) == [10, 20, 30]


def test_dedupe_all_same_keep_first():
    step = dedupe()
    assert step([5, 5, 5]) == [5]


def test_dedupe_all_same_keep_last():
    step = dedupe(keep="last")
    assert step([5, 5, 5]) == [5]


def test_dedupe_does_not_mutate_input():
    step = dedupe()
    original = [1, 2, 1, 3]
    copy = list(original)
    step(original)
    assert original == copy


# ---------------------------------------------------------------------------
# dedupe — with key function
# ---------------------------------------------------------------------------

def test_dedupe_with_key_keep_first():
    step = dedupe(key=lambda r: r["id"])
    data = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "c"}]
    result = step(data)
    assert result == [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


def test_dedupe_with_key_keep_last():
    step = dedupe(key=lambda r: r["id"], keep="last")
    data = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "c"}]
    result = step(data)
    assert result[0] == {"id": 1, "v": "c"}
    assert result[1] == {"id": 2, "v": "b"}


# ---------------------------------------------------------------------------
# dedupe — invalid arguments
# ---------------------------------------------------------------------------

def test_dedupe_invalid_keep_raises():
    with pytest.raises(ValueError, match="keep must be"):
        dedupe(keep="middle")


# ---------------------------------------------------------------------------
# dedupe_field
# ---------------------------------------------------------------------------

def test_dedupe_field_keep_first():
    step = dedupe_field("email")
    data = [
        {"email": "a@example.com", "name": "Alice"},
        {"email": "b@example.com", "name": "Bob"},
        {"email": "a@example.com", "name": "Alicia"},
    ]
    result = step(data)
    assert len(result) == 2
    assert result[0]["name"] == "Alice"


def test_dedupe_field_keep_last():
    step = dedupe_field("email", keep="last")
    data = [
        {"email": "a@example.com", "name": "Alice"},
        {"email": "b@example.com", "name": "Bob"},
        {"email": "a@example.com", "name": "Alicia"},
    ]
    result = step(data)
    assert len(result) == 2
    names = {r["name"] for r in result}
    assert "Alicia" in names
    assert "Alice" not in names


def test_dedupe_field_preserves_step_name():
    step = dedupe(key=lambda r: r["id"])
    assert step.__name__ == "dedupe"
