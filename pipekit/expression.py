"""Expression-based field filtering and transformation.

Provides a lightweight DSL for selecting and transforming records
using simple string expressions or callable predicates, without
requiring users to write full lambda functions for common cases.

Examples
--------
>>> from pipekit.expression import where, expr_step
>>> records = [
...     {"name": "alice", "score": 90},
...     {"name": "bob", "score": 55},
...     {"name": "carol", "score": 72},
... ]
>>> list(where("score > 70")(records))
[{'name': 'alice', 'score': 90}, {'name': 'carol', 'score': 72}]
"""

from __future__ import annotations

import operator
import re
from typing import Any, Callable, Iterable, Iterator

__all__ = ["where", "expr_field", "expr_step"]

# Supported binary operators for simple expression parsing
_OPS: dict[str, Callable[[Any, Any], bool]] = {
    ">=": operator.ge,
    "<=": operator.le,
    "!=": operator.ne,
    "==": operator.eq,
    ">": operator.gt,
    "<": operator.lt,
}

# Pattern: field OP literal  (string or number)
_EXPR_RE = re.compile(
    r"^\s*(\w+)\s*(>=|<=|!=|==|>|<)\s*(?P<q>['\"]?)(.+?)(?P=q)\s*$"
)


def _parse_expression(expr: str) -> Callable[[dict], bool]:
    """Parse a simple 'field OP value' expression string into a predicate.

    Supports numeric and string literals.  For anything more complex,
    pass a plain callable instead of a string.

    Parameters
    ----------
    expr:
        Expression string such as ``"score >= 70"`` or ``"status == 'ok'"``.

    Returns
    -------
    Callable[[dict], bool]
        A function that accepts a record dict and returns True/False.

    Raises
    ------
    ValueError
        If the expression cannot be parsed.
    """
    m = _EXPR_RE.match(expr)
    if not m:
        raise ValueError(
            f"Cannot parse expression {expr!r}. "
            "Expected format: 'field OP value' where OP is one of "
            + ", ".join(_OPS)
        )

    field, op_str, quote, raw_value = m.group(1), m.group(2), m.group("q"), m.group(4)
    op_fn = _OPS[op_str]

    # Determine the literal value type
    if quote:  # quoted string
        literal: Any = raw_value
    else:
        try:
            literal = int(raw_value)
        except ValueError:
            try:
                literal = float(raw_value)
            except ValueError:
                literal = raw_value  # fall back to plain string

    def predicate(record: dict) -> bool:
        return op_fn(record.get(field), literal)

    return predicate


def where(
    condition: str | Callable[[dict], bool],
) -> Callable[[Iterable[dict]], Iterator[dict]]:
    """Return a step that filters records matching *condition*.

    Parameters
    ----------
    condition:
        Either a string expression (e.g. ``"age >= 18"``) or any callable
        that takes a record dict and returns a truthy value.

    Returns
    -------
    Callable
        A pipeline step that yields only the matching records.
    """
    predicate = _parse_expression(condition) if isinstance(condition, str) else condition

    def transform(records: Iterable[dict]) -> Iterator[dict]:
        for record in records:
            if predicate(record):
                yield record

    transform.__name__ = f"where({condition!r})"
    return transform


def expr_field(
    output_field: str,
    expr: str | Callable[[dict], Any],
    *,
    overwrite: bool = True,
) -> Callable[[Iterable[dict]], Iterator[dict]]:
    """Derive a new field value from an expression or callable.

    Parameters
    ----------
    output_field:
        The field name to write the result into.
    expr:
        A callable ``(record) -> value`` or a string expression evaluated
        with the record fields available as local variables.
    overwrite:
        If False, skip records that already have a non-None value for
        *output_field*.  Defaults to True.

    Returns
    -------
    Callable
        A pipeline step that yields records with *output_field* populated.
    """
    if isinstance(expr, str):
        _expr_src = expr

        def _eval(record: dict) -> Any:  # noqa: ANN202
            return eval(_expr_src, {"__builtins__": {}}, dict(record))  # noqa: S307

        compute: Callable[[dict], Any] = _eval
    else:
        compute = expr

    def transform(records: Iterable[dict]) -> Iterator[dict]:
        for record in records:
            if not overwrite and record.get(output_field) is not None:
                yield record
                continue
            out = dict(record)
            out[output_field] = compute(record)
            yield out

    transform.__name__ = f"expr_field({output_field!r})"
    return transform


def expr_step(
    condition: str | Callable[[dict], bool],
) -> Callable[[Iterable[dict]], list[dict]]:
    """Eager (list-returning) variant of :func:`where`.

    Useful when you need a concrete list rather than a lazy iterator,
    e.g. when the result is passed to a step that requires ``len()``.

    Parameters
    ----------
    condition:
        String expression or callable predicate.

    Returns
    -------
    Callable
        A pipeline step that returns a list of matching records.
    """
    _filter = where(condition)

    def transform(records: Iterable[dict]) -> list[dict]:
        return list(_filter(records))

    transform.__name__ = f"expr_step({condition!r})"
    return transform
