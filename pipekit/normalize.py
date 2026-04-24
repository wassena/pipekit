"""Field normalization utilities for pipeline records."""

from typing import Any, Callable, Iterable, Optional


def normalize_field(
    field: str,
    method: str = "minmax",
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> Callable:
    """Return a step that normalizes a numeric field across all records.

    Methods:
        - ``minmax``: scale values to [0, 1] range.
        - ``zscore``: standardize using mean and standard deviation.

    Args:
        field: The record key to normalize.
        method: ``'minmax'`` (default) or ``'zscore'``.
        minimum: Pre-computed min (minmax only). Computed from data if omitted.
        maximum: Pre-computed max (minmax only). Computed from data if omitted.

    Returns:
        A step function ``(records) -> list``.
    """
    if method not in ("minmax", "zscore"):
        raise ValueError(f"Unknown normalization method: {method!r}")

    def transform(records: Iterable[dict]) -> list:
        rows = list(records)
        if not rows:
            return rows

        values = [r[field] for r in rows]

        if method == "minmax":
            lo = minimum if minimum is not None else min(values)
            hi = maximum if maximum is not None else max(values)
            span = hi - lo
            if span == 0:
                return [{**r, field: 0.0} for r in rows]
            return [{**r, field: (r[field] - lo) / span} for r in rows]

        # zscore
        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        std = variance ** 0.5
        if std == 0:
            return [{**r, field: 0.0} for r in rows]
        return [{**r, field: (r[field] - mean) / std} for r in rows]

    transform.__name__ = f"normalize_field({field!r}, method={method!r})"
    return transform


def clamp_field(field: str, lo: float, hi: float) -> Callable:
    """Return a step that clamps a numeric field to [lo, hi].

    Args:
        field: The record key to clamp.
        lo: Lower bound (inclusive).
        hi: Upper bound (inclusive).

    Returns:
        A step function ``(records) -> list``.
    """
    if lo > hi:
        raise ValueError(f"lo ({lo}) must be <= hi ({hi})")

    def transform(records: Iterable[dict]) -> list:
        return [{**r, field: max(lo, min(hi, r[field]))} for r in records]

    transform.__name__ = f"clamp_field({field!r}, {lo}, {hi})"
    return transform


def round_field(field: str, decimals: int = 2) -> Callable:
    """Return a step that rounds a numeric field to *decimals* places."""

    def transform(records: Iterable[dict]) -> list:
        return [{**r, field: round(r[field], decimals)} for r in records]

    transform.__name__ = f"round_field({field!r}, {decimals})"
    return transform
