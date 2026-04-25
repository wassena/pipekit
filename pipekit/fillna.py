"""Fill missing (None) values in records."""

from typing import Any, Callable, Dict, Iterable, List, Optional, Union


def fillna_field(
    field: str,
    value: Union[Any, Callable[[Dict], Any]],
    *,
    only_none: bool = True,
) -> Callable[[List[Dict]], List[Dict]]:
    """Return a transform that fills missing values in *field*.

    Args:
        field:     The record key to inspect.
        value:     A literal fill value or a callable ``(record) -> value``.
        only_none: When True (default) only fill ``None``; when False also
                   fill falsy values such as ``0``, ``""``, ``False``.

    Returns:
        A step function ``(records) -> records``.
    """
    def transform(records: List[Dict]) -> List[Dict]:
        out = []
        for record in records:
            current = record.get(field)
            needs_fill = current is None if only_none else not current
            if needs_fill:
                fill = value(record) if callable(value) else value
                out.append({**record, field: fill})
            else:
                out.append(record)
        return out

    transform.__name__ = f"fillna_field({field!r})"
    return transform


def fillna_fields(
    defaults: Dict[str, Any],
    *,
    only_none: bool = True,
) -> Callable[[List[Dict]], List[Dict]]:
    """Fill multiple fields at once using a mapping of field -> default.

    Args:
        defaults:  Mapping of field name to fill value (literals only).
        only_none: Same semantics as :func:`fillna_field`.

    Returns:
        A step function ``(records) -> records``.
    """
    def transform(records: List[Dict]) -> List[Dict]:
        out = []
        for record in records:
            patched = dict(record)
            for field, fill_value in defaults.items():
                current = patched.get(field)
                needs_fill = current is None if only_none else not current
                if needs_fill:
                    patched[field] = fill_value
            out.append(patched)
        return out

    transform.__name__ = "fillna_fields"
    return transform


def dropna(
    fields: Optional[List[str]] = None,
) -> Callable[[List[Dict]], List[Dict]]:
    """Drop records that contain None in any (or specified) fields.

    Args:
        fields: If provided, only check these fields; otherwise check all
                values in the record.

    Returns:
        A step function ``(records) -> records``.
    """
    def transform(records: List[Dict]) -> List[Dict]:
        out = []
        for record in records:
            values = (
                [record.get(f) for f in fields]
                if fields
                else list(record.values())
            )
            if None not in values:
                out.append(record)
        return out

    transform.__name__ = "dropna"
    return transform
