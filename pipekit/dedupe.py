"""
pipekit.dedupe
~~~~~~~~~~~~~~
Utilities for deduplicating records in a pipeline.
"""

from typing import Any, Callable, Hashable, Iterable, Iterator, List, Optional


def dedupe(
    key: Optional[Callable[[Any], Hashable]] = None,
    *,
    keep: str = "first",
) -> Callable[[Iterable[Any]], List[Any]]:
    """Return a step that removes duplicate records from a sequence.

    Args:
        key: A callable that extracts a hashable identity from each record.
             Defaults to the record itself (requires records to be hashable).
        keep: ``"first"`` (default) keeps the first occurrence;
              ``"last"`` keeps the last occurrence.

    Returns:
        A pipeline-compatible step function ``(records) -> list``.

    Example::

        from pipekit.dedupe import dedupe

        unique = dedupe(key=lambda r: r["id"])
        result = unique([{"id": 1}, {"id": 2}, {"id": 1}])
        # [{"id": 1}, {"id": 2}]
    """
    if keep not in ("first", "last"):
        raise ValueError(f"keep must be 'first' or 'last', got {keep!r}")

    _key: Callable[[Any], Hashable] = key if key is not None else (lambda x: x)

    def step(records: Iterable[Any]) -> List[Any]:
        if keep == "first":
            seen: set = set()
            result: List[Any] = []
            for record in records:
                k = _key(record)
                if k not in seen:
                    seen.add(k)
                    result.append(record)
            return result
        else:  # keep == "last"
            index: dict = {}
            ordered: List[Any] = []
            for record in records:
                k = _key(record)
                if k in index:
                    ordered[index[k]] = record
                else:
                    index[k] = len(ordered)
                    ordered.append(record)
            return ordered

    step.__name__ = "dedupe"
    step.__doc__ = f"Deduplicate records (keep={keep!r})."
    return step


def dedupe_field(field: str, *, keep: str = "first") -> Callable[[Iterable[Any]], List[Any]]:
    """Convenience wrapper: deduplicate dicts by a single field value.

    Args:
        field: The dict key to use as the uniqueness criterion.
        keep:  ``"first"`` or ``"last"`` — which duplicate to retain.

    Example::

        from pipekit.dedupe import dedupe_field

        unique = dedupe_field("email")
    """
    return dedupe(key=lambda r: r[field], keep=keep)
