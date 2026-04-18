"""Tests for pipekit.batch module."""

import pytest
from pipekit.batch import batch, process_batches, flat_map


# --- batch ---

def test_batch_even_split():
    result = list(batch([1, 2, 3, 4], 2))
    assert result == [[1, 2], [3, 4]]


def test_batch_remainder():
    result = list(batch([1, 2, 3, 4, 5], 2))
    assert result == [[1, 2], [3, 4], [5]]


def test_batch_larger_than_input():
    result = list(batch([1, 2], 10))
    assert result == [[1, 2]]


def test_batch_empty_input():
    result = list(batch([], 5))
    assert result == []


def test_batch_size_one():
    result = list(batch([1, 2, 3], 1))
    assert result == [[1], [2], [3]]


def test_batch_invalid_size():
    with pytest.raises(ValueError):
        list(batch([1, 2, 3], 0))


# --- process_batches ---

def test_process_batches_applies_transform():
    data = list(range(10))
    result = process_batches(data, lambda b: [x * 2 for x in b], size=3)
    assert result == [x * 2 for x in range(10)]


def test_process_batches_collects_all():
    data = ["a", "b", "c", "d", "e"]
    result = process_batches(data, lambda b: [s.upper() for s in b], size=2)
    assert result == ["A", "B", "C", "D", "E"]


def test_process_batches_error_propagates():
    def bad_transform(b):
        raise RuntimeError("fail")

    with pytest.raises(RuntimeError):
        process_batches([1, 2, 3], bad_transform, size=2)


def test_process_batches_on_error_callback():
    errors = []

    def on_error(exc, chunk):
        errors.append((str(exc), chunk))

    result = process_batches(
        [1, 2, 3],
        lambda b: (_ for _ in ()).throw(ValueError("oops")),
        size=2,
        on_error=on_error,
    )
    assert result == []
    assert len(errors) > 0


# --- flat_map ---

def test_flat_map_basic():
    result = flat_map([1, 2, 3], lambda x: [x, x * 10])
    assert result == [1, 10, 2, 20, 3, 30]


def test_flat_map_empty():
    result = flat_map([], lambda x: [x])
    assert result == []
