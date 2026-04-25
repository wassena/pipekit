"""coalesce.py — Fill missing or null field values from fallback sources."""

from typing import Any, Callable, Iterable, List, Optional


def coalesce_field(
    field: str,
    *fallbacks: Any,
    null_values: Optional[List[Any]] = None,
) -> Callable:
    """Return a step that fills *field* with the first non-null fallback value.

    Each entry in *fallbacks* may be:
    - a plain value  (used as-is)
    - a string       (treated as another field name to read from the record)
    - a callable     (called with the record; return value is used)

    Args:
        field:       The field to fill when its value is considered null.
        *fallbacks:  Ordered candidates tried left-to-right.
        null_values: Values treated as "missing" (default: [None]).
    """
    if null_values is None:
        null_values = [None]

    def _resolve(record: dict, candidate: Any) -> Any:
        if callable(candidate):
            return candidate(record)
        if isinstance(candidate, str) and candidate in record:
            return record[candidate]
        return candidate

    def transform(records: Iterable[dict]) -> List[dict]:
        out = []
        for record in records:
            if record.get(field) in null_values:
                filled = dict(record)
                for candidate in fallbacks:
                    value = _resolve(record, candidate)
                    if value not in null_values:
                        filled[field] = value
                        break
                out.append(filled)
            else:
                out.append(dict(record))
        return out

    return transform


def coalesce_fields(
    fields: List[str],
    default: Any = None,
    null_values: Optional[List[Any]] = None,
) -> Callable:
    """Return a step that ensures every field in *fields* has a non-null value,
    replacing missing/null values with *default*.

    Args:
        fields:      Field names to check and fill.
        default:     Replacement value when no better candidate exists.
        null_values: Values treated as "missing" (default: [None]).
    """
    if null_values is None:
        null_values = [None]

    def transform(records: Iterable[dict]) -> List[dict]:
        out = []
        for record in records:
            row = dict(record)
            for f in fields:
                if row.get(f) in null_values:
                    row[f] = default
            out.append(row)
        return out

    return transform
