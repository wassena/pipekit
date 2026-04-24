import pytest
from pipekit.sort import sort_by, sort_by_multiple, top_n


RECORDS = [
    {"name": "Alice", "age": 30, "score": 88},
    {"name": "Bob", "age": 25, "score": 95},
    {"name": "Carol", "age": 35, "score": 72},
    {"name": "Dave", "age": 25, "score": 80},
]


def test_sort_by_field_ascending():
    step = sort_by("age")
    result = step(RECORDS)
    ages = [r["age"] for r in result]
    assert ages == sorted(ages)


def test_sort_by_field_descending():
    step = sort_by("score", reverse=True)
    result = step(RECORDS)
    scores = [r["score"] for r in result]
    assert scores == sorted(scores, reverse=True)


def test_sort_by_callable():
    step = sort_by(lambda r: r["name"])
    result = step(RECORDS)
    names = [r["name"] for r in result]
    assert names == sorted(names)


def test_sort_by_does_not_mutate_original():
    original = [dict(r) for r in RECORDS]
    step = sort_by("age")
    step(RECORDS)
    assert RECORDS == original


def test_sort_by_empty_input():
    step = sort_by("age")
    assert step([]) == []


def test_sort_by_single_record():
    step = sort_by("age")
    result = step([{"age": 42}])
    assert result == [{"age": 42}]


def test_sort_by_multiple_fields():
    step = sort_by_multiple(["age", "score"])
    result = step(RECORDS)
    # Primary sort: age ascending; secondary: score ascending
    assert result[0]["age"] <= result[1]["age"]
    # Both age=25 entries: Bob(95) and Dave(80) — Dave should come first
    age_25 = [r for r in result if r["age"] == 25]
    assert age_25[0]["score"] < age_25[1]["score"]


def test_sort_by_multiple_with_reverse_tuple():
    step = sort_by_multiple(["age", ("score", True)])
    result = step(RECORDS)
    age_25 = [r for r in result if r["age"] == 25]
    # score descending: Bob(95) before Dave(80)
    assert age_25[0]["name"] == "Bob"
    assert age_25[1]["name"] == "Dave"


def test_sort_by_multiple_empty_input():
    step = sort_by_multiple(["age", "score"])
    assert step([]) == []


def test_top_n_returns_correct_count():
    step = top_n(2, "score")
    result = step(RECORDS)
    assert len(result) == 2


def test_top_n_returns_highest_by_default():
    step = top_n(2, "score")
    result = step(RECORDS)
    scores = [r["score"] for r in result]
    assert min(scores) >= sorted([r["score"] for r in RECORDS], reverse=True)[1]


def test_top_n_ascending():
    step = top_n(2, "score", reverse=False)
    result = step(RECORDS)
    scores = [r["score"] for r in result]
    assert max(scores) <= sorted([r["score"] for r in RECORDS])[1]


def test_top_n_larger_than_input_returns_all():
    step = top_n(100, "age")
    result = step(RECORDS)
    assert len(result) == len(RECORDS)


def test_top_n_empty_input():
    step = top_n(3, "age")
    assert step([]) == []
