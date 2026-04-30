import pytest
from pipekit.flag import flag_field, flag_if, flag_compare


# ---------------------------------------------------------------------------
# flag_field
# ---------------------------------------------------------------------------

def test_flag_field_sets_true_when_predicate_matches():
    records = [{"val": 10}, {"val": 3}]
    step = flag_field("val", lambda v: v > 5)
    result = step(records)
    assert result[0]["flagged"] is True
    assert result[1]["flagged"] is False


def test_flag_field_does_not_mutate_original():
    records = [{"val": 1}]
    step = flag_field("val", lambda v: v == 1)
    step(records)
    assert "flagged" not in records[0]


def test_flag_field_custom_flag_name():
    records = [{"score": 0.9}]
    step = flag_field("score", lambda v: v >= 0.8, flag_as="high_score")
    result = step(records)
    assert result[0]["high_score"] is True
    assert "flagged" not in result[0]


def test_flag_field_missing_field_value_is_none():
    records = [{"other": 1}]
    step = flag_field("val", lambda v: v is None)
    result = step(records)
    assert result[0]["flagged"] is True


def test_flag_field_no_overwrite_skips_existing():
    records = [{"val": 99, "flagged": False}]
    step = flag_field("val", lambda v: v > 10, overwrite=False)
    result = step(records)
    assert result[0]["flagged"] is False  # original preserved


def test_flag_field_overwrite_true_replaces_existing():
    records = [{"val": 99, "flagged": False}]
    step = flag_field("val", lambda v: v > 10, overwrite=True)
    result = step(records)
    assert result[0]["flagged"] is True


def test_flag_field_empty_input_returns_empty():
    step = flag_field("val", lambda v: True)
    assert step([]) == []


# ---------------------------------------------------------------------------
# flag_if
# ---------------------------------------------------------------------------

def test_flag_if_receives_full_record():
    records = [{"a": 1, "b": 2}, {"a": 5, "b": 2}]
    step = flag_if(lambda r: r["a"] + r["b"] > 5)
    result = step(records)
    assert result[0]["flagged"] is False
    assert result[1]["flagged"] is True


def test_flag_if_does_not_mutate_original():
    records = [{"x": 1}]
    step = flag_if(lambda r: True)
    step(records)
    assert "flagged" not in records[0]


def test_flag_if_no_overwrite():
    records = [{"x": 1, "flagged": True}]
    step = flag_if(lambda r: False, overwrite=False)
    result = step(records)
    assert result[0]["flagged"] is True


def test_flag_if_preserves_func_name_in_transform_name():
    def is_big(r):
        return r.get("val", 0) > 100

    step = flag_if(is_big, flag_as="big")
    assert "is_big" in step.__name__
    assert "big" in step.__name__


# ---------------------------------------------------------------------------
# flag_compare
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("op,val,expected", [
    (">",  5,  [True, False, False]),
    (">=", 5,  [True, True,  False]),
    ("<",  5,  [False, False, True]),
    ("<=", 5,  [False, True,  True]),
    ("==", 5,  [False, True,  False]),
    ("!=", 5,  [True,  False, True]),
])
def test_flag_compare_operators(op, val, expected):
    records = [{"n": 10}, {"n": 5}, {"n": 2}]
    step = flag_compare("n", op, val)
    result = step(records)
    assert [r["flagged"] for r in result] == expected


def test_flag_compare_invalid_op_raises():
    with pytest.raises(ValueError, match="Unsupported operator"):
        flag_compare("n", "??", 0)


def test_flag_compare_type_error_returns_false():
    records = [{"n": "text"}]
    step = flag_compare("n", ">", 5)
    result = step(records)
    assert result[0]["flagged"] is False
