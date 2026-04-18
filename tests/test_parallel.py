"""Tests for pipekit.parallel."""

import time
import pytest
from pipekit.parallel import parallel_map, parallel_step


def double(x):
    return x * 2


def slow_double(x):
    time.sleep(0.05)
    return x * 2


def boom(x):
    raise ValueError(f"bad value: {x}")


def test_parallel_map_basic():
    result = parallel_map(double, [1, 2, 3, 4])
    assert result == [2, 4, 6, 8]


def test_parallel_map_preserves_order():
    result = parallel_map(slow_double, list(range(10)), max_workers=4)
    assert result == [i * 2 for i in range(10)]


def test_parallel_map_empty():
    result = parallel_map(double, [])
    assert result == []


def test_parallel_map_single_item():
    result = parallel_map(double, [7])
    assert result == [14]


def test_parallel_map_raises_on_error():
    with pytest.raises(ValueError, match="bad value"):
        parallel_map(boom, [1])


def test_parallel_map_max_workers_one():
    result = parallel_map(double, [1, 2, 3], max_workers=1)
    assert result == [2, 4, 6]


def test_parallel_step_returns_callable():
    step = parallel_step(double)
    assert callable(step)


def test_parallel_step_applies_func():
    step = parallel_step(double, max_workers=2)
    result = step([1, 2, 3])
    assert result == [2, 4, 6]


def test_parallel_step_preserves_name():
    step = parallel_step(double)
    assert step.__name__ == "double"


def test_parallel_step_empty_list():
    step = parallel_step(double)
    assert step([]) == []


def test_parallel_step_raises_on_error():
    step = parallel_step(boom)
    with pytest.raises(ValueError, match="bad value"):
        step([42])


def test_parallel_map_is_faster_than_sequential():
    start = time.time()
    parallel_map(slow_double, list(range(8)), max_workers=4)
    elapsed = time.time() - start
    # 8 items * 0.05s / 4 workers ≈ 0.1s; sequential would be 0.4s
    assert elapsed < 0.3
