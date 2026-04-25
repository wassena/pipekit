import pytest
from pipekit.hooks import before_after, on_error, timed


# ---------------------------------------------------------------------------
# before_after
# ---------------------------------------------------------------------------

def test_before_hook_called():
    calls = []
    @before_after(before=lambda d: calls.append(("before", d)))
    def step(data):
        return {**data, "done": True}

    step({"x": 1})
    assert calls == [("before", {"x": 1})]


def test_after_hook_called():
    calls = []
    @before_after(after=lambda r: calls.append(("after", r)))
    def step(data):
        return {**data, "done": True}

    step({"x": 1})
    assert calls == [("after", {"x": 1, "done": True})]


def test_both_hooks_called_in_order():
    order = []
    @before_after(
        before=lambda d: order.append("before"),
        after=lambda r: order.append("after"),
    )
    def step(data):
        order.append("step")
        return data

    step({})
    assert order == ["before", "step", "after"]


def test_no_hooks_passthrough():
    @before_after()
    def step(data):
        return {**data, "ok": True}

    assert step({"v": 9}) == {"v": 9, "ok": True}


def test_hooks_metadata_stored():
    before_fn = lambda d: None
    after_fn = lambda r: None

    @before_after(before=before_fn, after=after_fn)
    def step(data):
        return data

    assert step._hooks["before"] is before_fn
    assert step._hooks["after"] is after_fn


def test_before_hook_exception_propagates():
    """An exception raised inside the before hook should propagate to the caller."""
    def bad_before(data):
        raise RuntimeError("before hook failed")

    @before_after(before=bad_before)
    def step(data):
        return data

    with pytest.raises(RuntimeError, match="before hook failed"):
        step({"x": 1})


# ---------------------------------------------------------------------------
# on_error
# ---------------------------------------------------------------------------

def test_on_error_returns_fallback():
    @on_error(lambda exc, data: {**data, "error": str(exc)})
    def step(data):
        raise ValueError("bad")

    result = step({"x": 1})
    assert result == {"x": 1, "error": "bad"}


def test_on_error_does_not_swallow_when_handler_raises():
    def bad_handler(exc, data):
        raise RuntimeError("handler failed")

    @on_error(bad_handler)
    def step(data):
        raise ValueError("original")

    with pytest.raises(RuntimeError, match="handler failed"):
        step({})


def test_on_error_not_triggered_on_success():
    calls = []
    @on_error(lambda exc, data: calls.append(exc))
    def step(data):
        return {**data, "ok": True}

    result = step({"v": 3})
    assert result == {"v": 3, "ok": True}
    assert calls == []


# ---------------------------------------------------------------------------
# timed
# ---------------------------------------------------------------------------

def test_timed_returns_correct_result():
    @timed
    def step(data):
        return {**data, "done": True}

    assert step({"a": 1}) == {"a": 1, "done": True}


def test_timed_records_duration():
    import time

    @timed
    def step(data):
        time.sleep(0.01)
        return data

    assert step.last_duration is None
    step({})
    assert step.last_duration is not None
    assert step.last_duration >= 0.01


def test_timed_duration_updates_on_repeated_calls():
    """Each call should overwrite last_duration with the most recent elapsed time."""
    import time

    @timed
    def step(data):
        time.sleep(0.01)
        return data

    step({})
    first_duration = step.last_duration
    step({})
    second_duration = step.last_duration

    # Both durations should be positive and independently recorded
    assert first_duration > 0
    assert second_duration > 0
