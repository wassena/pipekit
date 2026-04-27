"""Rate-limiting and record-count limiting utilities for pipelines."""

from __future__ import annotations

from typing import Any, Callable, Iterable, Iterator


def take_while(predicate: Callable[[Any], bool]) -> Callable[[Iterable], list]:
    """Return records from the front of the iterable while *predicate* is True.

    Stops at the first record that does not satisfy the predicate.

    Example::

        step = take_while(lambda r: r["score"] > 0.5)
        step([{"score": 0.9}, {"score": 0.7}, {"score": 0.3}, {"score": 0.8}])
        # -> [{"score": 0.9}, {"score": 0.7}]
    """

    def transform(records: Iterable[Any]) -> list[Any]:
        result = []
        for record in records:
            if not predicate(record):
                break
            result.append(record)
        return result

    return transform


def drop_while(predicate: Callable[[Any], bool]) -> Callable[[Iterable], list]:
    """Skip records from the front while *predicate* is True, then keep the rest.

    Example::

        step = drop_while(lambda r: r["score"] < 0.5)
        step([{"score": 0.2}, {"score": 0.4}, {"score": 0.6}, {"score": 0.8}])
        # -> [{"score": 0.6}, {"score": 0.8}]
    """

    def transform(records: Iterable[Any]) -> list[Any]:
        iterator: Iterator[Any] = iter(records)
        result = []
        dropping = True
        for record in iterator:
            if dropping and predicate(record):
                continue
            dropping = False
            result.append(record)
        return result

    return transform


def limit_by(field: str, max_value: Any) -> Callable[[Iterable], list]:
    """Keep only records where *field* is less than or equal to *max_value*.

    Records missing the field are passed through unchanged.

    Example::

        step = limit_by("age", 30)
        step([{"age": 25}, {"age": 35}, {"age": 30}])
        # -> [{"age": 25}, {"age": 30}]
    """

    def transform(records: Iterable[Any]) -> list[Any]:
        result = []
        for record in records:
            if field not in record or record[field] <= max_value:
                result.append(record)
        return result

    return transform


def cap_field(field: str, ceiling: Any) -> Callable[[Iterable], list]:
    """Clamp *field* values to *ceiling* without removing records.

    Records missing the field are left unchanged.

    Example::

        step = cap_field("score", 100)
        step([{"score": 120}, {"score": 80}, {"score": 100}])
        # -> [{"score": 100}, {"score": 80}, {"score": 100}]
    """

    def transform(records: Iterable[Any]) -> list[Any]:
        result = []
        for record in records:
            if field in record and record[field] > ceiling:
                record = {**record, field: ceiling}
            result.append(record)
        return result

    return transform
