"""Tests for pipekit.checkpoint."""

import json
import pytest
from pathlib import Path
from pipekit.checkpoint import checkpoint, clear_checkpoints, _checkpoint_path


@pytest.fixture
def cp_dir(tmp_path):
    return str(tmp_path / "checkpoints")


def test_checkpoint_runs_func_and_saves(cp_dir):
    calls = []

    @checkpoint("step1", checkpoint_dir=cp_dir)
    def step(data):
        calls.append(1)
        return [x * 2 for x in data]

    result = step([1, 2, 3])
    assert result == [2, 4, 6]
    assert len(calls) == 1


def test_checkpoint_reloads_without_calling_func(cp_dir):
    calls = []

    @checkpoint("step2", checkpoint_dir=cp_dir)
    def step(data):
        calls.append(1)
        return [x + 10 for x in data]

    step([1, 2])
    result = step([1, 2])
    assert result == [11, 12]
    assert len(calls) == 1


def test_checkpoint_overwrite_reruns_func(cp_dir):
    calls = []

    @checkpoint("step3", checkpoint_dir=cp_dir, overwrite=True)
    def step(data):
        calls.append(1)
        return data

    step([1])
    step([1])
    assert len(calls) == 2


def test_checkpoint_file_is_valid_json(cp_dir):
    @checkpoint("step4", checkpoint_dir=cp_dir)
    def step(data):
        return data

    step([{"a": 1}])
    path = _checkpoint_path(cp_dir, "step4")
    with open(path) as f:
        loaded = json.load(f)
    assert loaded == [{"a": 1}]


def test_clear_checkpoint_method(cp_dir):
    @checkpoint("step5", checkpoint_dir=cp_dir)
    def step(data):
        return data

    step([1, 2])
    step.clear_checkpoint()
    path = _checkpoint_path(cp_dir, "step5")
    assert not path.exists()


def test_clear_checkpoints_removes_all(cp_dir):
    @checkpoint("a", checkpoint_dir=cp_dir)
    def step_a(data): return data

    @checkpoint("b", checkpoint_dir=cp_dir)
    def step_b(data): return data

    step_a([1])
    step_b([2])
    removed = clear_checkpoints(cp_dir)
    assert removed == 2
    assert list(Path(cp_dir).glob("*.json")) == []


def test_clear_checkpoints_empty_dir(cp_dir):
    import os
    os.makedirs(cp_dir, exist_ok=True)
    assert clear_checkpoints(cp_dir) == 0
