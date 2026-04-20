"""splitter.py — Route records to different pipeline branches based on a predicate.

Provides `split`, which partitions data into two streams (matched / unmatched),
and `route`, which dispatches each record to the first branch whose predicate
returns True.

Example usage::

    from pipekit.splitter import split, route

    evens, odds = split(lambda x: x % 2 == 0, [1, 2, 3, 4, 5])
    # evens -> [2, 4],  odds -> [1, 3, 5]

    result = route(
        data=[{"score": 90}, {"score": 40}, {"score": 70}],
        branches=[
            (lambda r: r["score"] >= 80, lambda r: {**r, "grade": "A"}),
            (lambda r: r["score"] >= 60, lambda r: {**r, "grade": "B"}),
        ],
        default=lambda r: {**r, "grade": "F"},
    )
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, List, Optional, Tuple


def split(
    predicate: Callable[[Any], bool],
    data: Iterable[Any],
) -> Tuple[List[Any], List[Any]]:
    """Partition *data* into two lists based on *predicate*.

    Args:
        predicate: A callable that accepts a single record and returns a bool.
        data: An iterable of records to partition.

    Returns:
        A ``(matched, unmatched)`` tuple where *matched* contains every record
        for which ``predicate(record)`` is truthy and *unmatched* contains the
        rest.  The relative order of records within each list is preserved.

    Example::

        evens, odds = split(lambda x: x % 2 == 0, range(6))
        # evens -> [0, 2, 4],  odds -> [1, 3, 5]
    """
    matched: List[Any] = []
    unmatched: List[Any] = []
    for record in data:
        if predicate(record):
            matched.append(record)
        else:
            unmatched.append(record)
    return matched, unmatched


def route(
    data: Iterable[Any],
    branches: Iterable[Tuple[Callable[[Any], bool], Callable[[Any], Any]]],
    default: Optional[Callable[[Any], Any]] = None,
) -> List[Any]:
    """Dispatch each record in *data* to the first matching branch.

    Branches are evaluated in order; the transform of the first branch whose
    predicate returns ``True`` is applied to the record.  If no branch matches
    and *default* is provided, the default transform is applied.  Records with
    no matching branch and no default are silently dropped.

    Args:
        data: An iterable of records to route.
        branches: An iterable of ``(predicate, transform)`` pairs.  Each
            *predicate* is a callable ``record -> bool``; each *transform* is a
            callable ``record -> record``.
        default: Optional transform applied when no branch predicate matches.

    Returns:
        A flat list of transformed records in the same order as *data*.

    Example::

        result = route(
            data=[1, 2, 3, 4],
            branches=[
                (lambda x: x % 2 == 0, lambda x: x * 10),
            ],
            default=lambda x: x,
        )
        # result -> [1, 20, 3, 40]
    """
    branch_list = list(branches)  # allow re-iteration if a generator was passed
    results: List[Any] = []
    for record in data:
        for predicate, transform in branch_list:
            if predicate(record):
                results.append(transform(record))
                break
        else:
            # No branch matched.
            if default is not None:
                results.append(default(record))
            # else: record is dropped
    return results
