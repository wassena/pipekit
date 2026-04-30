"""flag.py — Conditional flagging utilities for pipekit pipelines.

Provides helpers to annotate records with boolean flags based on field
values or arbitrary predicates, without filtering them out.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable


def flag_field(
    field: str,
    predicate: Callable[[Any], bool],
    *,
    flag_as: str = "flagged",
    overwrite: bool = True,
) -> Callable[[Iterable[dict]], list[dict]]:
    """Add a boolean flag to each record based on a field's value.

    Args:
        field:     The source field to evaluate.
        predicate: A callable that receives the field value and returns bool.
        flag_as:   The name of the output flag field.
        overwrite: If False, skip records that already have the flag field set.

    Returns:
        A step function that accepts and returns a list of dicts.
    """
    def transform(records: Iterable[dict]) -> list[dict]:
        out = []
        for record in records:
            r = dict(record)
            if not overwrite and flag_as in r:
                out.append(r)
                continue
            r[flag_as] = predicate(r.get(field))
            out.append(r)
        return out

    transform.__name__ = f"flag_field({field!r} -> {flag_as!r})"
    return transform


def flag_if(
    predicate: Callable[[dict], bool],
    *,
    flag_as: str = "flagged",
    overwrite: bool = True,
) -> Callable[[Iterable[dict]], list[dict]]:
    """Add a boolean flag to each record based on the whole record.

    Args:
        predicate: A callable that receives the full record dict and returns bool.
        flag_as:   The name of the output flag field.
        overwrite: If False, skip records that already have the flag field set.

    Returns:
        A step function that accepts and returns a list of dicts.
    """
    def transform(records: Iterable[dict]) -> list[dict]:
        out = []
        for record in records:
            r = dict(record)
            if not overwrite and flag_as in r:
                out.append(r)
                continue
            r[flag_as] = predicate(r)
            out.append(r)
        return out

    transform.__name__ = f"flag_if({predicate.__name__} -> {flag_as!r})"
    return transform


def flag_compare(
    field: str,
    op: str,
    value: Any,
    *,
    flag_as: str = "flagged",
) -> Callable[[Iterable[dict]], list[dict]]:
    """Flag records where *field* satisfies a simple comparison.

    Supported ops: ``">"``, ``">="``, ``"<"``, ``"<="``, ``"=="``, ``"!="``.
    """
    _ops: dict[str, Callable[[Any, Any], bool]] = {
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }
    if op not in _ops:
        raise ValueError(f"Unsupported operator {op!r}. Choose from {list(_ops)}.")
    cmp = _ops[op]

    def predicate(v: Any) -> bool:
        try:
            return cmp(v, value)
        except TypeError:
            return False

    return flag_field(field, predicate, flag_as=flag_as)
