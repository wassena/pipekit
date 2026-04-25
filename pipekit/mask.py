"""Field masking and redaction utilities for sensitive data."""

from __future__ import annotations

import re
from typing import Any, Callable, Iterable


def mask_field(
    field: str,
    mask_with: str = "***",
    *,
    partial: bool = False,
    visible_chars: int = 4,
) -> Callable[[Iterable[dict]], list[dict]]:
    """Mask a field's value in every record.

    Args:
        field: The field name to mask.
        mask_with: Replacement string when not using partial masking.
        partial: If True, keep the last *visible_chars* characters visible.
        visible_chars: Number of trailing characters to expose when partial=True.

    Returns:
        A step function that masks the field in each record.
    """
    def transform(records: Iterable[dict]) -> list[dict]:
        out = []
        for record in records:
            r = dict(record)
            if field in r and r[field] is not None:
                value = str(r[field])
                if partial and len(value) > visible_chars:
                    r[field] = mask_with + value[-visible_chars:]
                else:
                    r[field] = mask_with
            out.append(r)
        return out

    transform.__name__ = f"mask_field({field!r})"
    return transform


def redact_pattern(
    field: str,
    pattern: str,
    replacement: str = "[REDACTED]",
) -> Callable[[Iterable[dict]], list[dict]]:
    """Replace regex pattern matches inside a string field.

    Args:
        field: The field name to scan.
        pattern: A regex pattern whose matches will be replaced.
        replacement: String to substitute for each match.

    Returns:
        A step function that applies the redaction to each record.
    """
    compiled = re.compile(pattern)

    def transform(records: Iterable[dict]) -> list[dict]:
        out = []
        for record in records:
            r = dict(record)
            if field in r and isinstance(r[field], str):
                r[field] = compiled.sub(replacement, r[field])
            out.append(r)
        return out

    transform.__name__ = f"redact_pattern({field!r})"
    return transform


def drop_fields(*fields: str) -> Callable[[Iterable[dict]], list[dict]]:
    """Remove one or more fields from every record.

    Args:
        *fields: Field names to drop.

    Returns:
        A step function that omits the specified fields.
    """
    field_set = set(fields)

    def transform(records: Iterable[dict]) -> list[dict]:
        return [{k: v for k, v in record.items() if k not in field_set} for record in records]

    transform.__name__ = f"drop_fields({', '.join(repr(f) for f in fields)})"
    return transform
