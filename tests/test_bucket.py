import pytest
from pipekit.bucket import bucket_by_thresholds, bucket_by_predicate, collect_buckets


# ---------------------------------------------------------------------------
# bucket_by_thresholds
# ---------------------------------------------------------------------------

def test_threshold_assigns_first_matching_bucket():
    step = bucket_by_thresholds("score", [("low", 40), ("mid", 70), ("high", 100)])
    result = step([{"score": 25}])
    assert result[0]["bucket"] == "low"


def test_threshold_assigns_mid_bucket():
    step = bucket_by_thresholds("score", [("low", 40), ("mid", 70), ("high", 100)])
    result = step([{"score": 55}])
    assert result[0]["bucket"] == "mid"


def test_threshold_uses_default_when_no_bound_matches():
    step = bucket_by_thresholds("score", [("low", 40), ("mid", 70)], default="high")
    result = step([{"score": 99}])
    assert result[0]["bucket"] == "high"


def test_threshold_none_value_uses_default():
    step = bucket_by_thresholds("score", [("low", 40)], default="unknown")
    result = step([{"score": None}])
    assert result[0]["bucket"] == "unknown"


def test_threshold_custom_output_field():
    step = bucket_by_thresholds("age", [("young", 30)], output_field="age_group")
    result = step([{"age": 20}])
    assert "age_group" in result[0]
    assert result[0]["age_group"] == "young"


def test_threshold_does_not_mutate_original():
    records = [{"score": 50}]
    step = bucket_by_thresholds("score", [("low", 40), ("mid", 70)])
    step(records)
    assert "bucket" not in records[0]


def test_threshold_empty_input_returns_empty():
    step = bucket_by_thresholds("score", [("low", 40)])
    assert step([]) == []


def test_threshold_boundary_value_exclusive():
    # value == bound should NOT match that bucket (strictly less than)
    step = bucket_by_thresholds("score", [("low", 40), ("mid", 70)], default="high")
    result = step([{"score": 40}])
    assert result[0]["bucket"] == "mid"


# ---------------------------------------------------------------------------
# bucket_by_predicate
# ---------------------------------------------------------------------------

def test_predicate_assigns_correct_bucket():
    step = bucket_by_predicate([
        ("negative", lambda r: r["val"] < 0),
        ("zero", lambda r: r["val"] == 0),
        ("positive", lambda r: r["val"] > 0),
    ])
    result = step([{"val": -5}, {"val": 0}, {"val": 3}])
    assert [r["bucket"] for r in result] == ["negative", "zero", "positive"]


def test_predicate_uses_default_when_no_match():
    step = bucket_by_predicate([("big", lambda r: r["x"] > 100)], default="small")
    result = step([{"x": 5}])
    assert result[0]["bucket"] == "small"


def test_predicate_first_match_wins():
    step = bucket_by_predicate([
        ("a", lambda r: r["x"] > 0),
        ("b", lambda r: r["x"] > 5),
    ])
    result = step([{"x": 10}])
    assert result[0]["bucket"] == "a"


def test_predicate_does_not_mutate_original():
    records = [{"x": 1}]
    step = bucket_by_predicate([("pos", lambda r: r["x"] > 0)])
    step(records)
    assert "bucket" not in records[0]


# ---------------------------------------------------------------------------
# collect_buckets
# ---------------------------------------------------------------------------

def test_collect_buckets_groups_records():
    records = [
        {"name": "a", "bucket": "low"},
        {"name": "b", "bucket": "high"},
        {"name": "c", "bucket": "low"},
    ]
    result = collect_buckets()(records)
    assert set(result.keys()) == {"low", "high"}
    assert len(result["low"]) == 2
    assert len(result["high"]) == 1


def test_collect_buckets_empty_input_returns_empty_dict():
    result = collect_buckets()([])
    assert result == {}


def test_collect_buckets_custom_field():
    records = [{"tier": "gold"}, {"tier": "silver"}]
    result = collect_buckets(output_field="tier")(records)
    assert "gold" in result and "silver" in result
