"""Batch processing utilities for pipekit pipelines."""

from typing import Callable, Iterable, Iterator, List, Optional


def batch(items: Iterable, size: int) -> Iterator[List]:
    """Yield successive chunks of `size` from `items`."""
    if size < 1:
        raise ValueError(f"Batch size must be >= 1, got {size}")
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def process_batches(
    items: Iterable,
    transform: Callable[[List], List],
    size: int = 100,
    on_error: Optional[Callable[[Exception, List], None]] = None,
) -> List:
    """Process `items` in batches using `transform`.

    Args:
        items: Input iterable.
        transform: Function applied to each batch (list -> list).
        size: Number of items per batch.
        on_error: Optional callback(exc, batch) called on failure.
                  If None, exceptions propagate.

    Returns:
        Flat list of all transformed results.
    """
    results = []
    for chunk in batch(items, size):
        try:
            results.extend(transform(chunk))
        except Exception as exc:
            if on_error is not None:
                on_error(exc, chunk)
            else:
                raise
    return results


def flat_map(items: Iterable, transform: Callable) -> List:
    """Apply `transform` to each item and flatten one level."""
    results = []
    for item in items:
        results.extend(transform(item))
    return results
