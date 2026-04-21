"""Sliding and tumbling window utilities for pipekit pipelines."""

from typing import Any, Callable, Iterable, Iterator, List, Optional


def sliding_window(
    data: List[Any],
    size: int,
    step: int = 1,
) -> Iterator[List[Any]]:
    """Yield overlapping windows of `size` over `data`, advancing by `step`.

    Args:
        data:  Input sequence.
        size:  Number of elements in each window.
        step:  How many elements to advance between windows (default 1).

    Raises:
        ValueError: If size or step are not positive integers.
    """
    if size < 1:
        raise ValueError(f"size must be >= 1, got {size}")
    if step < 1:
        raise ValueError(f"step must be >= 1, got {step}")

    for start in range(0, len(data) - size + 1, step):
        yield data[start : start + size]


def tumbling_window(
    data: List[Any],
    size: int,
) -> Iterator[List[Any]]:
    """Yield non-overlapping windows of exactly `size` elements.

    Trailing elements that do not fill a complete window are dropped.

    Args:
        data:  Input sequence.
        size:  Number of elements per window.

    Raises:
        ValueError: If size is not a positive integer.
    """
    if size < 1:
        raise ValueError(f"size must be >= 1, got {size}")

    return sliding_window(data, size, step=size)


def window_map(
    func: Callable[[List[Any]], Any],
    data: List[Any],
    size: int,
    step: int = 1,
) -> List[Any]:
    """Apply *func* to each sliding window and collect the results.

    This is a convenience wrapper around :func:`sliding_window` that is
    suitable for use as a :class:`pipekit.pipeline.Step` body.

    Args:
        func:  Callable that receives a window (list) and returns a value.
        data:  Input sequence.
        size:  Window size.
        step:  Advance step (default 1).

    Returns:
        List of results, one per window.
    """
    return [func(window) for window in sliding_window(data, size, step=step)]
