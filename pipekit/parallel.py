"""Parallel execution utilities for pipeline steps."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, Any, Optional


def parallel_map(
    func: Callable[[Any], Any],
    items: Iterable[Any],
    max_workers: int = 4,
    timeout: Optional[float] = None,
) -> list:
    """Apply func to each item in parallel using threads.

    Args:
        func: Callable to apply to each item.
        items: Iterable of inputs.
        max_workers: Number of worker threads.
        timeout: Optional timeout per future in seconds.

    Returns:
        List of results in the same order as inputs.

    Raises:
        Exception: Re-raises any exception raised by func.
    """
    items = list(items)
    results = [None] * len(items)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(func, item): idx for idx, item in enumerate(items)}
        for future in as_completed(futures, timeout=timeout):
            idx = futures[future]
            results[idx] = future.result()

    return results


def parallel_step(
    func: Callable[[Any], Any],
    max_workers: int = 4,
    timeout: Optional[float] = None,
) -> Callable[[list], list]:
    """Wrap a function as a pipeline step that processes a list in parallel.

    Args:
        func: Callable to apply to each item.
        max_workers: Number of worker threads.
        timeout: Optional timeout in seconds.

    Returns:
        A step function that accepts a list and returns a list.
    """
    def step(items: list) -> list:
        return parallel_map(func, items, max_workers=max_workers, timeout=timeout)

    step.__name__ = getattr(func, "__name__", "parallel_step")
    step.__doc__ = f"Parallel wrapper around {getattr(func, '__name__', func)}"
    return step
