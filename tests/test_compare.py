import pytest
from pipekit.compare import compare_field, compare_fields


# ---------------------------------------------------------------------------
# compare_field
# ---------------------------------------------------------------------------

def test_compare_field_eq_true():
    step = compare_field("x", "eq", 5)
    result = step([{"x": 5}])
    assert result[0]["x_eq"] is True


def test_compare_field_eq_false():
    step = compare_field("x", "eq", 5)
    result = step([{"x": 3}])
    assert result[0]["x_eq"] is False


def test_compare_field_ne():
    step = compare_field("x", "ne", 5)
    result = step([{"x": 3}])
    assert result[0]["x_ne"] is True


def test_compare_field_lt():
    step = compare_field("score", "lt", 10)
    result = step([{"score": 7}, {"score": 10}, {"score": 12}])
    assert [r["score_lt"] for r in result] == [True, False, False]


def test_compare_field_le():
    step = compare_field("score", "le", 10)
    result = step([{"score": 10}])
    assert result[0]["score_le"] is True


def test_compare_field_gt():
    step = compare_field("age", "gt", 18)
    result = step([{"age": 20}, {"age": 18}])
    assert result[0]["age_gt"] is True
    assert result[1]["age_gt"] is False


def test_compare_field_ge():
    step = compare_field("age", "ge", 18)
    result = step([{"age": 18}])
    assert result[0]["age_ge"] is True


def test_compare_field_in():
    step = compare_field("status", "in", ["active", "pending"])
    result = step([{"status": "active"}, {"status": "closed"}])
    assert result[0]["status_in"] is True
    assert result[1]["status_in"] is False


def test_compare_field_not_in():
    step = compare_field("status", "not_in", ["banned"])
    result = step([{"status": "active"}])
    assert result[0]["status_not_in"] is True


def test_compare_field_contains():
    step = compare_field("tags", "contains", "python")
    result = step([{"tags": ["python", "data"]}, {"tags": ["java"]}])
    assert result[0]["tags_contains"] is True
    assert result[1]["tags_contains"] is False


def test_compare_field_startswith():
    step = compare_field("name", "startswith", "Al")
    result = step([{"name": "Alice"}, {"name": "Bob"}])
    assert result[0]["name_startswith"] is True
    assert result[1]["name_startswith"] is False


def test_compare_field_endswith():
    step = compare_field("email", "endswith", ".com")
    result = step([{"email": "a@b.com"}])
    assert result[0]["email_endswith"] is True


def test_compare_field_custom_output_field():
    step = compare_field("x", "gt", 0, output_field="is_positive")
    result = step([{"x": 5}])
    assert "is_positive" in result[0]
    assert result[0]["is_positive"] is True


def test_compare_field_missing_uses_default():
    step = compare_field("x", "eq", None, missing_default=None)
    result = step([{"y": 1}])
    assert result[0]["x_eq"] is True


def test_compare_field_does_not_mutate_original():
    records = [{"x": 5}]
    step = compare_field("x", "eq", 5)
    step(records)
    assert records == [{"x": 5}]


def test_compare_field_empty_input():
    step = compare_field("x", "eq", 1)
    assert step([]) == []


def test_compare_field_unknown_operator_raises():
    with pytest.raises(ValueError, match="Unknown operator"):
        compare_field("x", "between", 5)


def test_compare_field_name():
    step = compare_field("score", "gt", 50)
    assert "score" in step.__name__ and "gt" in step.__name__


# ---------------------------------------------------------------------------
# compare_fields
# ---------------------------------------------------------------------------

def test_compare_fields_eq():
    step = compare_fields("a", "eq", "b")
    result = step([{"a": 3, "b": 3}, {"a": 3, "b": 4}])
    assert result[0]["a_vs_b"] is True
    assert result[1]["a_vs_b"] is False


def test_compare_fields_lt():
    step = compare_fields("price", "lt", "budget")
    result = step([{"price": 50, "budget": 100}])
    assert result[0]["price_vs_budget"] is True


def test_compare_fields_custom_output():
    step = compare_fields("a", "ge", "b", output_field="a_gte_b")
    result = step([{"a": 5, "b": 5}])
    assert result[0]["a_gte_b"] is True


def test_compare_fields_does_not_mutate():
    records = [{"a": 1, "b": 2}]
    step = compare_fields("a", "lt", "b")
    step(records)
    assert records == [{"a": 1, "b": 2}]


def test_compare_fields_unknown_operator_raises():
    with pytest.raises(ValueError):
        compare_fields("a", "in", "b")
