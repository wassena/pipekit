"""Field selection and projection utilities for pipeline records."""

from typing import Any, Callable, Iterable, Optional


def select_fields(
    fields: list[str],
    *,
    strict: bool = False,
) -> Callable[[Iterable[dict]], list[dict]]:
    """Return a step that keeps only the specified fields in each record.

    Args:
        fields: Field names to retain.
        strict: If True, raise KeyError when a field is missing from a record.

    Returns:
        A step function ``(records) -> records``.
    """
    def transform(records: Iterable[dict]) -> list[dict]:
        result = []
        for record in records:
            if strict:
                missing = [f for f in fields if f not in record]
                if missing:
                    raise KeyError(
                        f"select_fields: missing fields {missing} in record {record!r}"
                    )
            result.append({f: record[f] for f in fields if f in record})
        return result

    transform.__name__ = "select_fields"
    return transform


def exclude_fields(
    fields: list[str],
) -> Callable[[Iterable[dict]], list[dict]]:
    """Return a step that drops the specified fields from each record.

    Args:
        fields: Field names to remove.

    Returns:
        A step function ``(records) -> records``.
    """
    exclude = set(fields)

    def transform(records: Iterable[dict]) -> list[dict]:
        return [{k: v for k, v in record.items() if k not in exclude} for record in records]

    transform.__name__ = "exclude_fields"
    return transform


def select_if(
    predicate: Callable[[str, Any], bool],
) -> Callable[[Iterable[dict]], list[dict]]:
    """Return a step that keeps only fields satisfying *predicate(field_name, value)*.

    Args:
        predicate: Called with ``(field_name, value)``; field is kept when it
            returns ``True``.

    Returns:
        A step function ``(records) -> records``.
    """
    def transform(records: Iterable[dict]) -> list[dict]:
        return [
            {k: v for k, v in record.items() if predicate(k, v)}
            for record in records
        ]

    transform.__name__ = "select_if"
    return transform
