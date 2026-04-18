"""Field validation helpers for pipeline steps."""
from typing import Any, Callable, Dict, List, Optional


def validate_fields(required: List[str]) -> Callable:
    """Return a step transform that raises if required fields are missing."""
    def transform(record: Dict[str, Any]) -> Dict[str, Any]:
        missing = [f for f in required if f not in record]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        return record
    transform.__name__ = f"validate_fields({required})"
    return transform


def validate_type(field: str, expected_type: type) -> Callable:
    """Return a step transform that raises if a field is not the expected type."""
    def transform(record: Dict[str, Any]) -> Dict[str, Any]:
        if field in record and not isinstance(record[field], expected_type):
            raise TypeError(
                f"Field '{field}' expected {expected_type.__name__}, "
                f"got {type(record[field]).__name__}"
            )
        return record
    transform.__name__ = f"validate_type({field}, {expected_type.__name__})"
    return transform


def validate_range(
    field: str,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> Callable:
    """Return a step transform that raises if a numeric field is out of range."""
    def transform(record: Dict[str, Any]) -> Dict[str, Any]:
        if field in record:
            val = record[field]
            if min_val is not None and val < min_val:
                raise ValueError(f"Field '{field}' value {val} is below minimum {min_val}")
            if max_val is not None and val > max_val:
                raise ValueError(f"Field '{field}' value {val} is above maximum {max_val}")
        return record
    transform.__name__ = f"validate_range({field}, {min_val}, {max_val})"
    return transform


def validate_one_of(field: str, choices: List[Any]) -> Callable:
    """Return a step transform that raises if a field value is not in choices."""
    def transform(record: Dict[str, Any]) -> Dict[str, Any]:
        if field in record and record[field] not in choices:
            raise ValueError(
                f"Field '{field}' value {record[field]!r} not in allowed choices: {choices}"
            )
        return record
    transform.__name__ = f"validate_one_of({field}, {choices})"
    return transform
