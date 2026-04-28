"""Field formatting transforms for pipekit pipelines."""

from typing import Any, Callable, List, Optional


def format_field(
    field: str,
    template: str,
    *,
    missing: str = "",
) -> Callable[[List[dict]], List[dict]]:
    """Format a field's value using a Python format string.

    The record is passed as keyword arguments to the template.

    Args:
        field: The field to write the formatted value into.
        template: A Python format string, e.g. ``"{first} {last}"``.
        missing: Value to use when a referenced key is absent.

    Returns:
        A step function ``(records) -> records``.
    """
    def transform(records: List[dict]) -> List[dict]:
        result = []
        for record in records:
            row = dict(record)
            try:
                row[field] = template.format_map(_DefaultDict(row, missing))
            except (ValueError, KeyError):
                row[field] = missing
            result.append(row)
        return result

    transform.__name__ = f"format_field({field!r})"
    return transform


def format_number(
    field: str,
    fmt: str,
    *,
    on_error: Optional[Any] = None,
) -> Callable[[List[dict]], List[dict]]:
    """Apply a numeric format specifier to a field.

    Args:
        field: The field to format in-place.
        fmt: A format spec string such as ``".2f"`` or ``",d"``.
        on_error: Value to use when the field cannot be formatted.

    Returns:
        A step function ``(records) -> records``.
    """
    fmt_str = "{:" + fmt + "}"

    def transform(records: List[dict]) -> List[dict]:
        result = []
        for record in records:
            row = dict(record)
            if field in row:
                try:
                    row[field] = fmt_str.format(row[field])
                except (ValueError, TypeError):
                    if on_error is not None:
                        row[field] = on_error
            result.append(row)
        return result

    transform.__name__ = f"format_number({field!r}, {fmt!r})"
    return transform


def format_date(
    field: str,
    fmt: str,
    *,
    on_error: Optional[Any] = None,
) -> Callable[[List[dict]], List[dict]]:
    """Format a ``datetime`` or ``date`` object stored in a field.

    Args:
        field: The field containing the date/datetime object.
        fmt: A ``strftime``-compatible format string, e.g. ``"%Y-%m-%d"``.
        on_error: Value to use when formatting fails.

    Returns:
        A step function ``(records) -> records``.
    """
    def transform(records: List[dict]) -> List[dict]:
        result = []
        for record in records:
            row = dict(record)
            if field in row and row[field] is not None:
                try:
                    row[field] = row[field].strftime(fmt)
                except (AttributeError, ValueError):
                    if on_error is not None:
                        row[field] = on_error
            result.append(row)
        return result

    transform.__name__ = f"format_date({field!r}, {fmt!r})"
    return transform


class _DefaultDict(dict):
    """dict subclass that returns a default for missing keys."""

    def __init__(self, data: dict, default: Any):
        super().__init__(data)
        self._default = default

    def __missing__(self, key: str) -> Any:  # noqa: D105
        return self._default
