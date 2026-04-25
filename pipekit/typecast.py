"""Type casting utilities for pipeline records."""

from typing import Any, Callable, Dict, Iterable, List, Optional, Type


class CastError(ValueError):
    """Raised when a field cannot be cast to the target type."""


def _cast_value(value: Any, target: Type, strict: bool) -> Any:
    """Attempt to cast *value* to *target*; raise CastError on failure."""
    if value is None:
        return None
    try:
        return target(value)
    except (ValueError, TypeError) as exc:
        if strict:
            raise CastError(
                f"Cannot cast {value!r} to {target.__name__}: {exc}"
            ) from exc
        return value


def cast_field(
    field: str,
    target: Type,
    *,
    strict: bool = True,
    default: Any = None,
) -> Callable[[List[Dict]], List[Dict]]:
    """Return a step that casts *field* in every record to *target*.

    Parameters
    ----------
    field:   The record key to cast.
    target:  Any callable type such as ``int``, ``float``, ``str``, ``bool``.
    strict:  When *True* (default) raise :class:`CastError` on failure.
             When *False* leave the original value unchanged.
    default: Value to use when the field is missing from a record.
    """
    def transform(records: Iterable[Dict]) -> List[Dict]:
        out = []
        for record in records:
            rec = dict(record)
            value = rec.get(field, default)
            rec[field] = _cast_value(value, target, strict)
            out.append(rec)
        return out

    transform.__name__ = f"cast_field({field!r}, {target.__name__})"
    return transform


def cast_fields(
    schema: Dict[str, Type],
    *,
    strict: bool = True,
) -> Callable[[List[Dict]], List[Dict]]:
    """Return a step that casts multiple fields according to *schema*.

    Parameters
    ----------
    schema: Mapping of field name to target type.
    strict: Propagated to each individual :func:`cast_field` call.
    """
    def transform(records: Iterable[Dict]) -> List[Dict]:
        out = []
        for record in records:
            rec = dict(record)
            for field, target in schema.items():
                if field in rec:
                    rec[field] = _cast_value(rec[field], target, strict)
            out.append(rec)
        return out

    transform.__name__ = "cast_fields"
    return transform


def cast_step(
    field: str,
    target: Type,
    *,
    strict: bool = True,
) -> Callable[[List[Dict]], List[Dict]]:
    """Alias for :func:`cast_field` — named consistently with other ``*_step`` helpers."""
    return cast_field(field, target, strict=strict)
