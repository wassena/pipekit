"""Schema validation and coercion for pipeline records."""

from typing import Any, Callable, Dict, Optional


class SchemaError(ValueError):
    """Raised when a record does not conform to the schema."""
    pass


def _coerce(value: Any, expected_type: type) -> Any:
    try:
        return expected_type(value)
    except (ValueError, TypeError) as exc:
        raise SchemaError(
            f"Cannot coerce {value!r} to {expected_type.__name__}: {exc}"
        ) from exc


def apply_schema(
    schema: Dict[str, type],
    *,
    coerce: bool = False,
    allow_extra: bool = True,
) -> Callable[[dict], dict]:
    """Return a step that validates (and optionally coerces) a record.

    Args:
        schema:      Mapping of field name -> expected Python type.
        coerce:      If True, attempt to cast values to the expected type.
        allow_extra: If False, raise SchemaError for fields not in the schema.

    Returns:
        A step function ``(record: dict) -> dict``.
    """
    def transform(record: dict) -> dict:
        if not allow_extra:
            extra = set(record) - set(schema)
            if extra:
                raise SchemaError(f"Unexpected fields: {sorted(extra)}")

        out = dict(record)
        for field, expected_type in schema.items():
            if field not in out:
                raise SchemaError(f"Missing required field: {field!r}")
            value = out[field]
            if not isinstance(value, expected_type):
                if coerce:
                    out[field] = _coerce(value, expected_type)
                else:
                    raise SchemaError(
                        f"Field {field!r} expected {expected_type.__name__}, "
                        f"got {type(value).__name__}"
                    )
        return out

    transform.__name__ = "apply_schema"
    return transform


def schema_step(
    schema: Dict[str, type],
    *,
    coerce: bool = False,
    allow_extra: bool = True,
) -> Callable[[list], list]:
    """Return a step that applies *apply_schema* to every record in a list."""
    _transform = apply_schema(schema, coerce=coerce, allow_extra=allow_extra)

    def step(records: list) -> list:
        return [_transform(r) for r in records]

    step.__name__ = "schema_step"
    return step
