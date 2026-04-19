import pytest
from pipekit.fanout import fanout, fanout_dict, merge


def double(x):
    return x * 2


def negate(x):
    return -x


def length(x):
    return len(x)


# ---------------------------------------------------------------------------
# fanout
# ---------------------------------------------------------------------------

def test_fanout_returns_list_of_results():
    step = fanout(double, negate)
    assert step(5) == [10, -5]


def test_fanout_single_step():
    step = fanout(double)
    assert step(3) == [6]


def test_fanout_preserves_order():
    calls = []
    def a(x): calls.append("a"); return x + 1
    def b(x): calls.append("b"); return x + 2
    def c(x): calls.append("c"); return x + 3
    fanout(a, b, c)(0)
    assert calls == ["a", "b", "c"]


def test_fanout_raises_with_no_steps():
    with pytest.raises(ValueError, match="at least one step"):
        fanout()


def test_fanout_name_contains_step_names():
    step = fanout(double, negate)
    assert "double" in step.__name__
    assert "negate" in step.__name__


def test_fanout_works_with_non_numeric_data():
    step = fanout(length, list)
    assert step("hello") == [5, ["h", "e", "l", "l", "o"]]


# ---------------------------------------------------------------------------
# fanout_dict
# ---------------------------------------------------------------------------

def test_fanout_dict_returns_dict():
    step = fanout_dict(doubled=double, negated=negate)
    result = step(4)
    assert result == {"doubled": 8, "negated": -4}


def test_fanout_dict_keys_match_names():
    step = fanout_dict(a=double, b=negate)
    assert set(step(1).keys()) == {"a", "b"}


def test_fanout_dict_raises_with_no_steps():
    with pytest.raises(ValueError, match="at least one step"):
        fanout_dict()


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------

def test_merge_default_returns_list_unchanged():
    m = merge()
    assert m([1, 2, 3]) == [1, 2, 3]


def test_merge_with_combiner():
    m = merge(lambda r: r[0] + r[1])
    assert m([3, 4]) == 7


def test_fanout_then_merge_pipeline():
    from pipekit.pipeline import Pipeline
    pipe = Pipeline([
        fanout(double, negate),
        merge(sum),
    ])
    # double(5)=10, negate(5)=-5 => sum([10,-5])=5
    assert pipe(5) == 5
