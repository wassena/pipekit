"""Sorting utilities for pipeline records."""

from typing import Any, Callable, Iterable, List, Optional, Union


def sort_by(
    key: Union[str, Callable[[dict], Any]],
    reverse: bool = False,
) -> Callable[[List[dict]], List[dict]]:
    """Return a step that sorts records by a field name or key function.

    Args:
        key: A field name string or a callable that extracts a sort key.
        reverse: If True, sort in descending order.

    Returns:
        A step function that accepts and returns a list of records.

    Example::

        step = sort_by("age", reverse=True)
        result = step(records)
    """
    if isinstance(key, str):
        field = key
        key_fn: Callable[[dict], Any] = lambda r: r[field]
    else:
        key_fn = key

    def transform(records: List[dict]) -> List[dict]:
        return sorted(records, key=key_fn, reverse=reverse)

    transform.__name__ = "sort_by"
    return transform


def sort_by_multiple(
    keys: List[Union[str, tuple]],
) -> Callable[[List[dict]], List[dict]]:
    """Sort records by multiple fields, each optionally reversed.

    Args:
        keys: A list of field names or (field, reverse) tuples.

    Returns:
        A step function that sorts records by each key in order.

    Example::

        step = sort_by_multiple(["department", ("salary", True)])
        result = step(records)
    """
    parsed: List[tuple] = []
    for k in keys:
        if isinstance(k, str):
            parsed.append((k, False))
        else:
            parsed.append((k[0], k[1]))

    def transform(records: List[dict]) -> List[dict]:
        result = list(records)
        # Apply sorts in reverse order so primary key wins
        for field, rev in reversed(parsed):
            result = sorted(result, key=lambda r: r[field], reverse=rev)
        return result

    transform.__name__ = "sort_by_multiple"
    return transform


def top_n(
    n: int,
    key: Union[str, Callable[[dict], Any]],
    reverse: bool = True,
) -> Callable[[List[dict]], List[dict]]:
    """Return the top N records by a key.

    Args:
        n: Number of records to return.
        key: Field name or callable for ranking.
        reverse: If True (default), highest values rank first.

    Returns:
        A step function returning at most n records.
    """
    sorter = sort_by(key, reverse=reverse)

    def transform(records: List[dict]) -> List[dict]:
        return sorter(records)[:n]

    transform.__name__ = "top_n"
    return transform
