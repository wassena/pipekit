"""Utilities for flattening nested data structures in a pipeline."""

from typing import Any, Callable, Iterable, List, Optional


def flatten(data: Iterable[Any], depth: int = 1) -> List[Any]:
    """Flatten a nested iterable up to *depth* levels.

    Args:
        data: An iterable that may contain nested iterables.
        depth: How many levels deep to flatten. Use -1 for unlimited depth.

    Returns:
        A flat list of items.

    Example::

        flatten([[1, 2], [3, [4, 5]]])          # [1, 2, 3, [4, 5]]
        flatten([[1, 2], [3, [4, 5]]], depth=2) # [1, 2, 3, 4, 5]
        flatten([[1, [2, [3]]]], depth=-1)      # [1, 2, 3]
    """
    result: List[Any] = []
    for item in data:
        if isinstance(item, (list, tuple)) and depth != 0:
            result.extend(flatten(item, depth - 1 if depth > 0 else -1))
        else:
            result.append(item)
    return result


def flatten_field(field: str, depth: int = 1) -> Callable[[List[dict]], List[dict]]:
    """Return a step that flattens a list-valued field in each record.

    Each record's *field* must contain a list. The field is replaced with a
    flattened version of that list.

    Args:
        field: The key whose value (a list) should be flattened.
        depth: Depth passed to :func:`flatten`.

    Returns:
        A pipeline step (callable) that accepts and returns a list of dicts.
    """
    def transform(records: List[dict]) -> List[dict]:
        out = []
        for record in records:
            new_record = dict(record)
            new_record[field] = flatten(record[field], depth=depth)
            out.append(new_record)
        return out
    transform.__name__ = f"flatten_field({field!r})"
    return transform


def flatten_records(field: str, depth: int = 1) -> Callable[[List[dict]], List[dict]]:
    """Expand a list-valued field so each nested item becomes its own record.

    Given records like ``{"tags": ["a", "b"], "id": 1}``, this step produces
    one record per tag while copying all other fields.

    Args:
        field: The key whose list values should be expanded into new records.
        depth: Flatten the list to this depth before expanding.

    Returns:
        A pipeline step that returns an expanded list of dicts.
    """
    def transform(records: List[dict]) -> List[dict]:
        out = []
        for record in records:
            items = flatten(record[field], depth=depth)
            for item in items:
                new_record = {k: v for k, v in record.items() if k != field}
                new_record[field] = item
                out.append(new_record)
        return out
    transform.__name__ = f"flatten_records({field!r})"
    return transform
