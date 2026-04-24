import pytest
from pipekit.normalize import normalize_field, clamp_field, round_field


# ---------------------------------------------------------------------------
# normalize_field — minmax
# ---------------------------------------------------------------------------

def test_minmax_scales_to_zero_one():
    records = [{"v": 0}, {"v": 5}, {"v": 10}]
    result = normalize_field("v")(records)
    assert result[0]["v"] == pytest.approx(0.0)
    assert result[1]["v"] == pytest.approx(0.5)
    assert result[2]["v"] == pytest.approx(1.0)


def test_minmax_does_not_mutate_originals():
    records = [{"v": 1}, {"v": 3}]
    normalize_field("v")(records)
    assert records[0]["v"] == 1
    assert records[1]["v"] == 3


def test_minmax_constant_values_returns_zero():
    records = [{"v": 7}, {"v": 7}, {"v": 7}]
    result = normalize_field("v")(records)
    assert all(r["v"] == pytest.approx(0.0) for r in result)


def test_minmax_with_preset_bounds():
    records = [{"v": 2}, {"v": 6}]
    result = normalize_field("v", minimum=0, maximum=10)(records)
    assert result[0]["v"] == pytest.approx(0.2)
    assert result[1]["v"] == pytest.approx(0.6)


def test_minmax_empty_input():
    assert normalize_field("v")([]) == []


# ---------------------------------------------------------------------------
# normalize_field — zscore
# ---------------------------------------------------------------------------

def test_zscore_mean_is_zero():
    records = [{"v": 10}, {"v": 20}, {"v": 30}]
    result = normalize_field("v", method="zscore")(records)
    mean = sum(r["v"] for r in result) / len(result)
    assert mean == pytest.approx(0.0, abs=1e-9)


def test_zscore_constant_values_returns_zero():
    records = [{"v": 5}, {"v": 5}]
    result = normalize_field("v", method="zscore")(records)
    assert all(r["v"] == pytest.approx(0.0) for r in result)


def test_invalid_method_raises():
    with pytest.raises(ValueError, match="Unknown normalization method"):
        normalize_field("v", method="magic")


# ---------------------------------------------------------------------------
# clamp_field
# ---------------------------------------------------------------------------

def test_clamp_keeps_values_in_range():
    records = [{"x": -5}, {"x": 3}, {"x": 15}]
    result = clamp_field("x", 0, 10)(records)
    assert [r["x"] for r in result] == [0, 3, 10]


def test_clamp_does_not_mutate():
    records = [{"x": 99}]
    clamp_field("x", 0, 10)(records)
    assert records[0]["x"] == 99


def test_clamp_invalid_bounds_raises():
    with pytest.raises(ValueError, match="lo"):
        clamp_field("x", 10, 0)


# ---------------------------------------------------------------------------
# round_field
# ---------------------------------------------------------------------------

def test_round_field_default_decimals():
    records = [{"n": 3.14159}]
    result = round_field("n")(records)
    assert result[0]["n"] == 3.14


def test_round_field_custom_decimals():
    records = [{"n": 1.23456}]
    result = round_field("n", decimals=4)(records)
    assert result[0]["n"] == 1.2346


def test_round_field_does_not_mutate():
    records = [{"n": 2.71828}]
    round_field("n")(records)
    assert records[0]["n"] == 2.71828
