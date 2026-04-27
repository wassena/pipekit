"""Field interpolation utilities for pipekit pipelines.

Provides functions to fill in missing or sparse values in a sequence of
records using linear or forward/backward-fill interpolation strategies.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional


def _ffill(values: list) -> list:
    """Forward-fill None values using the last known value."""
    result = []
    last = None
    for v in values:
        if v is not None:
            last = v
        result.append(last)
    return result


def _bfill(values: list) -> list:
    """Backward-fill None values using the next known value."""
    return list(reversed(_ffill(list(reversed(values)))))


def _linear(values: list) -> list:
    """Linearly interpolate None values between known numeric values."""
    result = list(values)
    n = len(result)
    i = 0
    while i < n:
        if result[i] is None:
            # find the previous known index
            left = i - 1
            # find the next known index
            right = i
            while right < n and result[right] is None:
                right += 1
            if left < 0 and right >= n:
                # all None — leave as-is
                pass
            elif left < 0:
                # no left anchor — use bfill for this segment
                for k in range(i, right):
                    result[k] = result[right]
            elif right >= n:
                # no right anchor — use ffill for this segment
                for k in range(i, n):
                    result[k] = result[left]
            else:
                left_val = result[left]
                right_val = result[right]
                steps = right - left
                for k in range(i, right):
                    t = (k - left) / steps
                    result[k] = left_val + t * (right_val - left_val)
            i = right
        else:
            i += 1
    return result


_STRATEGIES: dict[str, Callable[[list], list]] = {
    "ffill": _ffill,
    "bfill": _bfill,
    "linear": _linear,
}


def interpolate_field(
    field: str,
    strategy: str = "ffill",
    missing: Any = None,
) -> Callable[[List[dict]], List[dict]]:
    """Return a step that interpolates *field* across a list of records.

    Args:
        field:    The record key to interpolate.
        strategy: One of ``'ffill'``, ``'bfill'``, or ``'linear'``.
        missing:  Sentinel value treated as "missing" (default ``None``).

    Returns:
        A transform callable ``(records) -> records``.

    Example::

        step = interpolate_field("temperature", strategy="linear")
        result = step(records)
    """
    if strategy not in _STRATEGIES:
        raise ValueError(
            f"Unknown strategy {strategy!r}. Choose from: {list(_STRATEGIES)}"
        )
    fn = _STRATEGIES[strategy]

    def transform(records: List[dict]) -> List[dict]:
        values = [
            None if r.get(field) == missing and r.get(field) is missing else r.get(field)
            for r in records
        ]
        # normalise sentinel to None for strategy functions
        normalised = [None if v == missing else v for v in values]
        filled = fn(normalised)
        return [{**r, field: v} for r, v in zip(records, filled)]

    transform.__name__ = f"interpolate_field({field!r}, strategy={strategy!r})"
    return transform


def interpolate_step(
    fields: List[str],
    strategy: str = "ffill",
    missing: Any = None,
) -> Callable[[List[dict]], List[dict]]:
    """Interpolate multiple fields in one step.

    Args:
        fields:   List of field names to interpolate.
        strategy: Interpolation strategy (``'ffill'``, ``'bfill'``, ``'linear'``).
        missing:  Sentinel value treated as missing.

    Returns:
        A transform callable ``(records) -> records``.
    """
    steps = [interpolate_field(f, strategy=strategy, missing=missing) for f in fields]

    def transform(records: List[dict]) -> List[dict]:
        result = records
        for step in steps:
            result = step(result)
        return result

    transform.__name__ = f"interpolate_step({fields!r}, strategy={strategy!r})"
    return transform
