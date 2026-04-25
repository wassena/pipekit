"""Truncate and slice utilities for pipekit pipelines."""

from typing import Any, Callable, Iterable, List, Optional


def take(n: int) -> Callable[[List[Any]], List[Any]]:
    """Return a step that keeps only the first *n* records.

    Args:
        n: Maximum number of records to keep.  Must be >= 0.

    Returns:
        A pipeline step (list -> list).

    Raises:
        ValueError: If *n* is negative.
    """
    if n < 0:
        raise ValueError(f"n must be >= 0, got {n}")

    def transform(records: List[Any]) -> List[Any]:
        return list(records[:n])

    transform.__name__ = f"take({n})"
    return transform


def drop(n: int) -> Callable[[List[Any]], List[Any]]:
    """Return a step that skips the first *n* records.

    Args:
        n: Number of records to drop.  Must be >= 0.

    Returns:
        A pipeline step (list -> list).

    Raises:
        ValueError: If *n* is negative.
    """
    if n < 0:
        raise ValueError(f"n must be >= 0, got {n}")

    def transform(records: List[Any]) -> List[Any]:
        return list(records[n:])

    transform.__name__ = f"drop({n})"
    return transform


def slice_records(
    start: int = 0,
    stop: Optional[int] = None,
    step: int = 1,
) -> Callable[[List[Any]], List[Any]]:
    """Return a step that slices records like Python's built-in slice.

    Args:
        start: Start index (inclusive, default 0).
        stop:  Stop index (exclusive, default None means end of list).
        step:  Step between elements (default 1).

    Returns:
        A pipeline step (list -> list).

    Raises:
        ValueError: If *step* is 0.
    """
    if step == 0:
        raise ValueError("step must not be 0")

    def transform(records: List[Any]) -> List[Any]:
        return list(records[start:stop:step])

    transform.__name__ = f"slice_records({start}, {stop}, {step})"
    return transform


def take_while(
    predicate: Callable[[Any], bool],
) -> Callable[[List[Any]], List[Any]]:
    """Return a step that keeps leading records while *predicate* is True.

    Args:
        predicate: Callable that receives a record and returns bool.

    Returns:
        A pipeline step (list -> list).
    """
    def transform(records: List[Any]) -> List[Any]:
        result = []
        for record in records:
            if not predicate(record):
                break
            result.append(record)
        return result

    transform.__name__ = "take_while"
    return transform


def drop_while(
    predicate: Callable[[Any], bool],
) -> Callable[[List[Any]], List[Any]]:
    """Return a step that skips leading records while *predicate* is True.

    Args:
        predicate: Callable that receives a record and returns bool.

    Returns:
        A pipeline step (list -> list).
    """
    def transform(records: List[Any]) -> List[Any]:
        it: Iterable[Any] = iter(records)
        result = []
        dropping = True
        for record in it:
            if dropping and predicate(record):
                continue
            dropping = False
            result.append(record)
        return result

    transform.__name__ = "drop_while"
    return transform
