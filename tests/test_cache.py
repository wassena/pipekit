"""Tests for pipekit.cache module."""

import os
import pickle
import pytest

from pipekit.cache import cached_step, clear_all_cache, _make_key


@pytest.fixture
def cache_dir(tmp_path):
    return str(tmp_path / "test_cache")


def test_make_key_is_deterministic():
    k1 = _make_key("fn", (1, 2), {"a": 3})
    k2 = _make_key("fn", (1, 2), {"a": 3})
    assert k1 == k2


def test_make_key_differs_on_different_args():
    k1 = _make_key("fn", (1,), {})
    k2 = _make_key("fn", (2,), {})
    assert k1 != k2


def test_cached_step_stores_result(cache_dir):
    call_count = {"n": 0}

    @cached_step(cache_dir=cache_dir)
    def step(data):
        call_count["n"] += 1
        return data * 2

    result1 = step(5)
    result2 = step(5)
    assert result1 == 10
    assert result2 == 10
    assert call_count["n"] == 1


def test_cached_step_different_args_not_shared(cache_dir):
    @cached_step(cache_dir=cache_dir)
    def step(data):
        return data + 1

    assert step(1) == 2
    assert step(2) == 3


def test_cached_step_disabled_skips_cache(cache_dir):
    call_count = {"n": 0}

    @cached_step(cache_dir=cache_dir, enabled=False)
    def step(data):
        call_count["n"] += 1
        return data

    step(42)
    step(42)
    assert call_count["n"] == 2
    assert not os.path.exists(cache_dir)


def test_cached_step_creates_cache_dir(cache_dir):
    @cached_step(cache_dir=cache_dir)
    def step(x):
        return x

    step("hello")
    assert os.path.isdir(cache_dir)


def test_clear_cache_removes_files(cache_dir):
    @cached_step(cache_dir=cache_dir)
    def step(x):
        return x

    step(1)
    step(2)
    removed = clear_all_cache(cache_dir)
    assert removed == 2
    assert len(os.listdir(cache_dir)) == 0


def test_clear_cache_nonexistent_dir_returns_zero():
    removed = clear_all_cache("/tmp/nonexistent_pipekit_xyz")
    assert removed == 0


def test_clear_cache_via_wrapper(cache_dir):
    @cached_step(cache_dir=cache_dir)
    def step(x):
        return x

    step(99)
    removed = step.clear_cache()
    assert removed == 1
