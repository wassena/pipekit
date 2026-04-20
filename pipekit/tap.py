"""tap.py — Side-effect steps that observe data without modifying it."""

from typing import Any, Callable, Iterable, Optional


def tap(func: Callable[[Any], None]) -> Callable[[Any], Any]:
    """Wrap *func* as a pass-through step.

    *func* is called with the data for its side-effect (logging, metrics,
    saving a snapshot, …).  The original data is returned unchanged so the
    step can be dropped anywhere in a pipeline without altering the flow.

    Example::

        from pipekit.tap import tap

        inspect = tap(lambda data: print(f"records: {len(data)}"))
        pipeline = Pipeline([load, clean, inspect, enrich, save])
    """
    def decorator(data: Any) -> Any:
        func(data)
        return data

    decorator.__name__ = getattr(func, "__name__", "tap")
    decorator.__doc__ = getattr(func, "__doc__", None)
    return decorator


def tap_each(func: Callable[[Any], None]) -> Callable[[Iterable[Any]], Iterable[Any]]:
    """Apply *func* to every element of an iterable, then return the list.

    Useful for per-record side-effects (e.g. emitting metrics per row)
    without breaking the pipeline's data flow.

    Example::

        log_row = tap_each(lambda row: print(row))
    """
    def decorator(data: Iterable[Any]) -> list:
        result = []
        for item in data:
            func(item)
            result.append(item)
        return result

    decorator.__name__ = getattr(func, "__name__", "tap_each")
    return decorator


def tap_if(
    predicate: Callable[[Any], bool],
    func: Callable[[Any], None],
) -> Callable[[Any], Any]:
    """Call *func* only when *predicate(data)* is truthy; always return data.

    Example::

        warn_empty = tap_if(lambda d: len(d) == 0, lambda d: print("WARNING: empty"))
    """
    def decorator(data: Any) -> Any:
        if predicate(data):
            func(data)
        return data

    decorator.__name__ = getattr(func, "__name__", "tap_if")
    return decorator
