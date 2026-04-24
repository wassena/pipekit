import pytest
from pipekit.pivot import pivot, melt, pivot_step, melt_step


# ---------------------------------------------------------------------------
# pivot
# ---------------------------------------------------------------------------

def test_pivot_basic():
    records = [
        {"name": "alice", "metric": "height", "val": 165},
        {"name": "alice", "metric": "weight", "val": 60},
        {"name": "bob",   "metric": "height", "val": 180},
        {"name": "bob",   "metric": "weight", "val": 80},
    ]
    result = pivot(records, index="name", column="metric", value="val")
    assert len(result) == 2
    alice = next(r for r in result if r["name"] == "alice")
    assert alice["height"] == 165
    assert alice["weight"] == 60


def test_pivot_preserves_index_order():
    records = [
        {"id": "z", "k": "a", "v": 1},
        {"id": "m", "k": "a", "v": 2},
        {"id": "a", "k": "a", "v": 3},
    ]
    result = pivot(records, index="id", column="k", value="v")
    assert [r["id"] for r in result] == ["z", "m", "a"]


def test_pivot_missing_cell_is_none():
    records = [
        {"name": "alice", "metric": "height", "val": 165},
        {"name": "bob",   "metric": "weight", "val": 80},
    ]
    result = pivot(records, index="name", column="metric", value="val")
    alice = next(r for r in result if r["name"] == "alice")
    assert alice["weight"] is None


def test_pivot_with_agg_sum():
    records = [
        {"dept": "eng", "month": "jan", "sales": 10},
        {"dept": "eng", "month": "jan", "sales": 5},
        {"dept": "eng", "month": "feb", "sales": 20},
    ]
    result = pivot(records, index="dept", column="month", value="sales", agg=sum)
    row = result[0]
    assert row["jan"] == 15
    assert row["feb"] == 20


def test_pivot_empty_input():
    assert pivot([], index="a", column="b", value="c") == []


def test_pivot_does_not_mutate_input():
    records = [{"id": 1, "k": "x", "v": 99}]
    original = [dict(r) for r in records]
    pivot(records, index="id", column="k", value="v")
    assert records == original


# ---------------------------------------------------------------------------
# melt
# ---------------------------------------------------------------------------

def test_melt_basic():
    records = [{"name": "alice", "height": 165, "weight": 60}]
    result = melt(records, id_fields=["name"])
    assert len(result) == 2
    variables = {r["variable"] for r in result}
    assert variables == {"height", "weight"}
    height_row = next(r for r in result if r["variable"] == "height")
    assert height_row["value"] == 165
    assert height_row["name"] == "alice"


def test_melt_custom_column_and_value_names():
    records = [{"id": 1, "a": 10, "b": 20}]
    result = melt(records, id_fields=["id"], column_name="metric", value_name="score")
    assert all("metric" in r and "score" in r for r in result)


def test_melt_explicit_value_fields():
    records = [{"id": 1, "a": 10, "b": 20, "c": 30}]
    result = melt(records, id_fields=["id"], value_fields=["a", "b"])
    assert len(result) == 2
    assert all(r["variable"] in {"a", "b"} for r in result)


def test_melt_empty_input():
    assert melt([], id_fields=["id"]) == []


def test_melt_does_not_mutate_input():
    records = [{"id": 1, "x": 5}]
    original = [dict(r) for r in records]
    melt(records, id_fields=["id"])
    assert records == original


# ---------------------------------------------------------------------------
# step wrappers
# ---------------------------------------------------------------------------

def test_pivot_step_is_callable():
    records = [
        {"name": "alice", "metric": "score", "val": 42},
    ]
    step = pivot_step(index="name", column="metric", value="val")
    result = step(records)
    assert result[0]["score"] == 42


def test_melt_step_is_callable():
    records = [{"id": 7, "x": 1, "y": 2}]
    step = melt_step(id_fields=["id"])
    result = step(records)
    assert len(result) == 2
    assert all(r["id"] == 7 for r in result)


def test_pivot_melt_roundtrip():
    """Pivot then melt should recover the original shape (values may differ in order)."""
    long_records = [
        {"name": "alice", "metric": "height", "val": 165},
        {"name": "alice", "metric": "weight", "val": 60},
        {"name": "bob",   "metric": "height", "val": 180},
        {"name": "bob",   "metric": "weight", "val": 80},
    ]
    wide = pivot(long_records, index="name", column="metric", value="val")
    back = melt(wide, id_fields=["name"], column_name="metric", value_name="val")
    assert len(back) == len(long_records)
    for orig in long_records:
        match = next(
            r for r in back
            if r["name"] == orig["name"] and r["metric"] == orig["metric"]
        )
        assert match["val"] == orig["val"]
