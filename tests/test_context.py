"""Tests for pipekit.context."""

import pytest
from pipekit.context import PipelineContext, with_context


def test_context_stores_and_retrieves_values():
    ctx = PipelineContext()
    ctx.set("key", "value")
    assert ctx.get("key") == "value"


def test_context_default_when_missing():
    ctx = PipelineContext()
    assert ctx.get("missing") is None
    assert ctx.get("missing", 99) == 99


def test_context_has_returns_true_when_present():
    ctx = PipelineContext()
    ctx.set("x", 1)
    assert ctx.has("x") is True


def test_context_has_returns_false_when_absent():
    ctx = PipelineContext()
    assert ctx.has("x") is False


def test_context_all_returns_copy():
    ctx = PipelineContext()
    ctx.set("a", 1)
    ctx.set("b", 2)
    result = ctx.all()
    assert result == {"a": 1, "b": 2}
    result["a"] = 999
    assert ctx.get("a") == 1  # original unaffected


def test_context_metadata_passed_at_construction():
    ctx = PipelineContext(run_id="abc", env="test")
    assert ctx.metadata["run_id"] == "abc"
    assert ctx.metadata["env"] == "test"


def test_context_metadata_is_immutable_copy():
    ctx = PipelineContext(run_id="abc")
    m = ctx.metadata
    m["run_id"] = "hacked"
    assert ctx.metadata["run_id"] == "abc"


def test_with_context_injects_context():
    ctx = PipelineContext()

    @with_context(ctx)
    def my_step(data, context):
        context.set("seen", True)
        return data

    result = my_step([1, 2, 3])
    assert result == [1, 2, 3]
    assert ctx.get("seen") is True


def test_with_context_preserves_function_name():
    ctx = PipelineContext()

    @with_context(ctx)
    def named_step(data, context):
        return data

    assert named_step.__name__ == "named_step"


def test_with_context_works_in_pipeline():
    from pipekit.pipeline import Pipeline

    ctx = PipelineContext(run_id="test-run")

    @with_context(ctx)
    def count_records(data, context):
        context.set("count", len(data))
        return data

    @with_context(ctx)
    def tag_records(data, context):
        run_id = context.metadata["run_id"]
        return [{**r, "run_id": run_id} for r in data]

    pipeline = Pipeline([count_records, tag_records])
    records = [{"x": 1}, {"x": 2}]
    result = pipeline(records)

    assert ctx.get("count") == 2
    assert all(r["run_id"] == "test-run" for r in result)
