"""Tests for pipekit.sample."""

import pytest

from pipekit.sample import sample, sample_step, reservoir_sample


# ---------------------------------------------------------------------------
# sample()
# ---------------------------------------------------------------------------

def test_sample_by_n_returns_correct_count():
    data = list(range(100))
    result = sample(data, n=10, seed=0)
    assert len(result) == 10


def test_sample_by_fraction_returns_correct_count():
    data = list(range(100))
    result = sample(data, fraction=0.2, seed=0)
    assert len(result) == 20


def test_sample_is_reproducible_with_seed():
    data = list(range(50))
    r1 = sample(data, n=10, seed=42)
    r2 = sample(data, n=10, seed=42)
    assert r1 == r2


def test_sample_differs_with_different_seeds():
    data = list(range(50))
    r1 = sample(data, n=10, seed=1)
    r2 = sample(data, n=10, seed=2)
    assert r1 != r2


def test_sample_n_larger_than_data_returns_all():
    data = list(range(5))
    result = sample(data, n=100, seed=0)
    assert len(result) == 5


def test_sample_fraction_zero_returns_empty():
    data = list(range(20))
    result = sample(data, fraction=0.0, seed=0)
    assert result == []


def test_sample_fraction_one_returns_all():
    data = list(range(20))
    result = sample(data, fraction=1.0, seed=0)
    assert len(result) == 20


def test_sample_does_not_mutate_input():
    data = list(range(10))
    original = list(data)
    sample(data, n=5, seed=0)
    assert data == original


def test_sample_raises_without_n_or_fraction():
    with pytest.raises(ValueError, match="Specify either"):
        sample(list(range(10)))


def test_sample_raises_on_invalid_fraction():
    with pytest.raises(ValueError, match="fraction"):
        sample(list(range(10)), fraction=1.5)


def test_sample_empty_input_returns_empty():
    assert sample([], n=5, seed=0) == []


# ---------------------------------------------------------------------------
# sample_step()
# ---------------------------------------------------------------------------

def test_sample_step_is_callable():
    step = sample_step(n=3, seed=0)
    assert callable(step)


def test_sample_step_returns_correct_count():
    step = sample_step(n=5, seed=7)
    result = step(list(range(50)))
    assert len(result) == 5


def test_sample_step_fraction():
    step = sample_step(fraction=0.5, seed=0)
    result = step(list(range(100)))
    assert len(result) == 50


# ---------------------------------------------------------------------------
# reservoir_sample()
# ---------------------------------------------------------------------------

def test_reservoir_sample_returns_k_items():
    data = list(range(1000))
    result = reservoir_sample(data, k=50, seed=0)
    assert len(result) == 50


def test_reservoir_sample_k_larger_than_data():
    data = list(range(5))
    result = reservoir_sample(data, k=20, seed=0)
    assert len(result) == 5


def test_reservoir_sample_k_zero_returns_empty():
    result = reservoir_sample(list(range(10)), k=0)
    assert result == []


def test_reservoir_sample_reproducible():
    data = list(range(200))
    r1 = reservoir_sample(data, k=30, seed=99)
    r2 = reservoir_sample(data, k=30, seed=99)
    assert r1 == r2


def test_reservoir_sample_raises_on_negative_k():
    with pytest.raises(ValueError, match="non-negative"):
        reservoir_sample(list(range(10)), k=-1)
