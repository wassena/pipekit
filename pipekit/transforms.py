"""Built-in reusable transform functions for common data operations."""

from typing import Any, Callable, Iterable


def map_field(field: str, func: Callable) -> Callable:
    """Return a step function that applies func to a specific field in a dict."""
    def transform(data: dict) -> dict:
        result = dict(data)
        result[field] = func(data[field])
        return result
    transform.__name__ = f"map_field({field!r})"
    return transform


def filter_field(field: str, predicate: Callable) -> Callable:
    """Return a step function that filters a list of dicts by a field predicate."""
    def transform(data: Iterable[dict]) -> list:
        return [item for item in data if predicate(item[field])]
    transform.__name__ = f"filter_field({field!r})"
    return transform


def rename_field(old: str, new: str) -> Callable:
    """Return a step function that renames a key in a dict."""
    def transform(data: dict) -> dict:
        result = dict(data)
        result[new] = result.pop(old)
        return result
    transform.__name__ = f"rename_field({old!r} -> {new!r})"
    return transform


def drop_fields(*fields: str) -> Callable:
    """Return a step function that removes specified keys from a dict."""
    def transform(data: dict) -> dict:
        return {k: v for k, v in data.items() if k not in fields}
    transform.__name__ = f"drop_fields({', '.join(repr(f) for f in fields)})"
    return transform


def apply_to_each(func: Callable) -> Callable:
    """Return a step function that applies func to each item in an iterable."""
    def transform(data: Iterable) -> list:
        return [func(item) for item in data]
    transform.__name__ = f"apply_to_each({getattr(func, '__name__', repr(func))})"
    return transform


def add_field(field: str, value_func: Callable) -> Callable:
    """Return a step function that adds a new field derived from the dict."""
    def transform(data: dict) -> dict:
        result = dict(data)
        result[field] = value_func(data)
        return result
    transform.__name__ = f"add_field({field!r})"
    return transform
