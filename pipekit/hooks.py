"""Lifecycle hooks for pipeline steps."""

from functools import wraps
from typing import Callable, Optional
import time


def before_after(
    before: Optional[Callable] = None,
    after: Optional[Callable] = None,
):
    """
    Wrap a step function with before/after hooks.

    Args:
        before: Called with (data,) before the step runs.
        after:  Called with (result,) after the step runs.

    Returns:
        Decorator that attaches hooks to a step function.

    Example::

        def log_in(data):
            print(f"in:  {data}")

        def log_out(result):
            print(f"out: {result}")

        @before_after(before=log_in, after=log_out)
        def my_step(data):
            return {**data, "processed": True}
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(data):
            if before is not None:
                before(data)
            result = fn(data)
            if after is not None:
                after(result)
            return result
        wrapper._hooks = {"before": before, "after": after}
        return wrapper
    return decorator


def on_error(handler: Callable):
    """
    Attach an error handler to a step.

    The handler receives (exception, data) and may return a fallback value.
    If the handler raises, that exception propagates.

    Example::

        @on_error(lambda exc, data: data)
        def risky_step(data):
            raise ValueError("oops")
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(data):
            try:
                return fn(data)
            except Exception as exc:  # noqa: BLE001
                return handler(exc, data)
        wrapper._error_handler = handler
        return wrapper
    return decorator


def timed(fn: Callable) -> Callable:
    """
    Record wall-clock execution time on the returned callable.

    The elapsed seconds are stored in ``wrapper.last_duration`` after each call.
    """
    @wraps(fn)
    def wrapper(data):
        start = time.perf_counter()
        result = fn(data)
        wrapper.last_duration = time.perf_counter() - start
        return result
    wrapper.last_duration = None
    return wrapper
