"""Field-level comparison utilities for pipekit pipelines."""

from typing import Any, Callable, List, Optional


def compare_field(
    field: str,
    operator: str,
    value: Any,
    *,
    output_field: Optional[str] = None,
    missing_default: Any = None,
) -> Callable:
    """Return a step that adds a boolean field based on a field comparison.

    Supported operators: ``eq``, ``ne``, ``lt``, ``le``, ``gt``, ``ge``,
    ``in``, ``not_in``, ``contains``, ``startswith``, ``endswith``.

    Args:
        field: Source field name.
        operator: Comparison operator string.
        value: Value to compare against.
        output_field: Destination field (defaults to ``<field>_<operator>``).
        missing_default: Value used when *field* is absent.

    Returns:
        A pipeline step function.
    """
    _OPS = {
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
        "lt": lambda a, b: a < b,
        "le": lambda a, b: a <= b,
        "gt": lambda a, b: a > b,
        "ge": lambda a, b: a >= b,
        "in": lambda a, b: a in b,
        "not_in": lambda a, b: a not in b,
        "contains": lambda a, b: b in a,
        "startswith": lambda a, b: str(a).startswith(b),
        "endswith": lambda a, b: str(a).endswith(b),
    }
    if operator not in _OPS:
        raise ValueError(f"Unknown operator '{operator}'. Choose from: {sorted(_OPS)}.")

    op_fn = _OPS[operator]
    dest = output_field or f"{field}_{operator}"

    def transform(records: List[dict]) -> List[dict]:
        result = []
        for rec in records:
            out = dict(rec)
            raw = rec.get(field, missing_default)
            out[dest] = op_fn(raw, value)
            result.append(out)
        return result

    transform.__name__ = f"compare_field({field!r}, {operator!r})"
    return transform


def compare_fields(
    left: str,
    operator: str,
    right: str,
    *,
    output_field: Optional[str] = None,
) -> Callable:
    """Return a step that compares two fields within the same record.

    Args:
        left: Left-hand field name.
        operator: Comparison operator string (same set as :func:`compare_field`).
        right: Right-hand field name.
        output_field: Destination field (defaults to ``<left>_vs_<right>``).

    Returns:
        A pipeline step function.
    """
    _OPS = {
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
        "lt": lambda a, b: a < b,
        "le": lambda a, b: a <= b,
        "gt": lambda a, b: a > b,
        "ge": lambda a, b: a >= b,
    }
    if operator not in _OPS:
        raise ValueError(f"Unknown operator '{operator}'. Choose from: {sorted(_OPS)}.")

    op_fn = _OPS[operator]
    dest = output_field or f"{left}_vs_{right}"

    def transform(records: List[dict]) -> List[dict]:
        result = []
        for rec in records:
            out = dict(rec)
            out[dest] = op_fn(rec.get(left), rec.get(right))
            result.append(out)
        return result

    transform.__name__ = f"compare_fields({left!r}, {operator!r}, {right!r})"
    return transform
