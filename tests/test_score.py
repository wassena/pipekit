import pytest
from pipekit.score import score_field, score_by, rank_by


# ---------------------------------------------------------------------------
# score_field
# ---------------------------------------------------------------------------

def test_score_field_basic_weighted_sum():
    records = [{"a": 2, "b": 3}]
    step = score_field({"a": 1.0, "b": 2.0})
    result = step(records)
    assert result[0]["score"] == pytest.approx(8.0)  # 2*1 + 3*2


def test_score_field_custom_output_field():
    records = [{"x": 5}]
    step = score_field({"x": 3.0}, output_field="total")
    result = step(records)
    assert "total" in result[0]
    assert result[0]["total"] == pytest.approx(15.0)


def test_score_field_missing_uses_default():
    records = [{"a": 4}]
    step = score_field({"a": 1.0, "b": 2.0}, missing=0.0)
    result = step(records)
    assert result[0]["score"] == pytest.approx(4.0)


def test_score_field_missing_custom_default():
    records = [{"a": 4}]
    step = score_field({"a": 1.0, "b": 2.0}, missing=1.0)
    result = step(records)
    assert result[0]["score"] == pytest.approx(6.0)  # 4*1 + 1*2


def test_score_field_normalise():
    records = [{"a": 10, "b": 10}]
    step = score_field({"a": 1.0, "b": 1.0}, normalise=True)
    result = step(records)
    assert result[0]["score"] == pytest.approx(10.0)  # (10+10)/2


def test_score_field_does_not_mutate_original():
    records = [{"a": 1, "b": 2}]
    original = dict(records[0])
    step = score_field({"a": 1.0, "b": 1.0})
    step(records)
    assert records[0] == original


def test_score_field_empty_input():
    step = score_field({"a": 1.0})
    assert step([]) == []


def test_score_field_none_value_uses_missing():
    records = [{"a": None, "b": 5}]
    step = score_field({"a": 2.0, "b": 1.0}, missing=0.0)
    result = step(records)
    assert result[0]["score"] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# score_by
# ---------------------------------------------------------------------------

def test_score_by_applies_func():
    records = [{"price": 10, "qty": 3}]
    step = score_by(lambda r: r["price"] * r["qty"])
    result = step(records)
    assert result[0]["score"] == pytest.approx(30.0)


def test_score_by_custom_output_field():
    records = [{"v": 7}]
    step = score_by(lambda r: r["v"] ** 2, output_field="squared")
    result = step(records)
    assert result[0]["squared"] == pytest.approx(49.0)


def test_score_by_does_not_mutate_original():
    records = [{"v": 3}]
    original = dict(records[0])
    step = score_by(lambda r: r["v"])
    step(records)
    assert records[0] == original


def test_score_by_multiple_records():
    records = [{"n": i} for i in range(5)]
    step = score_by(lambda r: r["n"] * 2)
    result = step(records)
    assert [r["score"] for r in result] == [0, 2, 4, 6, 8]


# ---------------------------------------------------------------------------
# rank_by
# ---------------------------------------------------------------------------

def test_rank_by_descending_default():
    records = [{"score": 10}, {"score": 30}, {"score": 20}]
    step = rank_by("score")
    result = step(records)
    assert result[0]["rank"] == 3
    assert result[1]["rank"] == 1
    assert result[2]["rank"] == 2


def test_rank_by_ascending():
    records = [{"score": 10}, {"score": 30}, {"score": 20}]
    step = rank_by("score", ascending=True)
    result = step(records)
    assert result[0]["rank"] == 1
    assert result[1]["rank"] == 3
    assert result[2]["rank"] == 2


def test_rank_by_dense_ties():
    records = [{"score": 10}, {"score": 10}, {"score": 5}]
    step = rank_by("score")
    result = step(records)
    assert result[0]["rank"] == 1
    assert result[1]["rank"] == 1
    assert result[2]["rank"] == 2


def test_rank_by_custom_start():
    records = [{"score": 1}, {"score": 2}]
    step = rank_by("score", start=0)
    result = step(records)
    assert result[0]["rank"] == 1  # lower score, descending
    assert result[1]["rank"] == 0


def test_rank_by_custom_output_field():
    records = [{"v": 5}]
    step = rank_by("v", output_field="position")
    result = step(records)
    assert "position" in result[0]


def test_rank_by_does_not_mutate_original():
    records = [{"v": 1}]
    original = dict(records[0])
    step = rank_by("v")
    step(records)
    assert records[0] == original


def test_rank_by_empty_input():
    step = rank_by("score")
    assert step([]) == []
