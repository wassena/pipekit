import pytest
from pipekit.join import inner_join, left_join, join_step


# ── inner_join ───────────────────────────────────────────────────────────────

def test_inner_join_basic():
    left = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    right = [{"id": 1, "extra": "x"}, {"id": 3, "extra": "z"}]
    result = inner_join(left, right, on="id")
    assert len(result) == 1
    assert result[0] == {"id": 1, "val": "a", "extra": "x"}


def test_inner_join_multiple_matches():
    left = [{"id": 1, "val": "a"}]
    right = [{"id": 1, "extra": "x"}, {"id": 1, "extra": "y"}]
    result = inner_join(left, right, on="id")
    assert len(result) == 2
    extras = {r["extra"] for r in result}
    assert extras == {"x", "y"}


def test_inner_join_no_matches_returns_empty():
    left = [{"id": 1}]
    right = [{"id": 99}]
    assert inner_join(left, right, on="id") == []


def test_inner_join_empty_inputs():
    assert inner_join([], [{"id": 1}], on="id") == []
    assert inner_join([{"id": 1}], [], on="id") == []


def test_inner_join_collision_uses_suffixes():
    left = [{"id": 1, "name": "alice"}]
    right = [{"id": 1, "name": "bob"}]
    result = inner_join(left, right, on="id", suffixes=("_l", "_r"))
    assert result[0]["name_l"] == "alice"
    assert result[0]["name_r"] == "bob"


def test_inner_join_does_not_mutate_inputs():
    left = [{"id": 1, "val": "a"}]
    right = [{"id": 1, "extra": "x"}]
    left_copy = [{**r} for r in left]
    inner_join(left, right, on="id")
    assert left == left_copy


# ── left_join ────────────────────────────────────────────────────────────────

def test_left_join_keeps_unmatched_left():
    left = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    right = [{"id": 1, "extra": "x"}]
    result = left_join(left, right, on="id")
    assert len(result) == 2
    unmatched = next(r for r in result if r["id"] == 2)
    assert unmatched["extra"] is None


def test_left_join_matched_record_is_enriched():
    left = [{"id": 1, "val": "a"}]
    right = [{"id": 1, "extra": "x"}]
    result = left_join(left, right, on="id")
    assert result[0]["extra"] == "x"


def test_left_join_empty_right_returns_left_with_nulls():
    left = [{"id": 1, "val": "a"}]
    result = left_join(left, [], on="id")
    assert len(result) == 1
    assert result[0]["id"] == 1


def test_left_join_empty_left_returns_empty():
    right = [{"id": 1, "extra": "x"}]
    assert left_join([], right, on="id") == []


# ── join_step ────────────────────────────────────────────────────────────────

def test_join_step_inner_works_as_pipeline_step():
    right = [{"id": 1, "score": 99}]
    step = join_step(right, on="id", how="inner")
    data = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    result = step(data)
    assert len(result) == 1
    assert result[0]["score"] == 99


def test_join_step_left_works_as_pipeline_step():
    right = [{"id": 1, "score": 99}]
    step = join_step(right, on="id", how="left")
    data = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    result = step(data)
    assert len(result) == 2


def test_join_step_invalid_how_raises():
    with pytest.raises(ValueError, match="Unsupported join type"):
        join_step([], on="id", how="outer")


def test_join_step_has_descriptive_name():
    step = join_step([], on="id", how="inner")
    assert "inner" in step.__name__
    assert "id" in step.__name__
