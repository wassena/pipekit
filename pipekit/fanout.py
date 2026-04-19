"""fanout.py — run multiple steps on the same input and collect results."""

from __future__ import annotations

from typing import Any, Callable, Dict, List


def fanout(*steps: Callable[[Any], Any]) -> Callable[[Any], List[Any]]:
    """Return a step that passes *data* through every step and returns a list
    of results in the same order as the steps were declared.

    Example::

        summarise = fanout(count_records, compute_mean, compute_max)
        results = summarise(data)  # [count, mean, max]
    """
    if not steps:
        raise ValueError("fanout requires at least one step")

    def _run(data: Any) -> List[Any]:
        return [step(data) for step in steps]

    _run.__name__ = "fanout({})".format(", ".join(getattr(s, "__name__", repr(s)) for s in steps))
    return _run


def fanout_dict(**named_steps: Callable[[Any], Any]) -> Callable[[Any], Dict[str, Any]]:
    """Like :func:`fanout` but returns a *dict* keyed by the argument names.

    Example::

        analyse = fanout_dict(count=count_records, mean=compute_mean)
        result = analyse(data)  # {"count": ..., "mean": ...}
    """
    if not named_steps:
        raise ValueError("fanout_dict requires at least one step")

    def _run(data: Any) -> Dict[str, Any]:
        return {name: step(data) for name, step in named_steps.items()}

    _run.__name__ = "fanout_dict({})".format(", ".join(named_steps))
    return _run


def merge(combiner: Callable[[List[Any]], Any] = None) -> Callable[[List[Any]], Any]:
    """Return a step that merges a list of results (output of :func:`fanout`)
    using *combiner*.  Defaults to returning the list unchanged.

    Example::

        pipeline = Pipeline([
            fanout(step_a, step_b),
            merge(lambda results: {"a": results[0], "b": results[1]}),
        ])
    """
    def _run(results: List[Any]) -> Any:
        if combiner is None:
            return results
        return combiner(results)

    _run.__name__ = "merge"
    return _run
