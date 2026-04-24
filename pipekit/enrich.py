"""Field enrichment utilities for pipeline records."""

from typing import Any, Callable, Dict, Iterable, List, Optional


def enrich_field(
    field: str,
    func: Callable[[Dict], Any],
    *,
    overwrite: bool = True,
) -> Callable[[List[Dict]], List[Dict]]:
    """Add or update a field on each record using a derived value.

    Args:
        field: The field name to set on each record.
        func: A callable that receives the full record and returns the new value.
        overwrite: If False, skip records that already have the field set.

    Returns:
        A step function that enriches a list of records.
    """
    def transform(records: List[Dict]) -> List[Dict]:
        result = []
        for record in records:
            if not overwrite and field in record:
                result.append(record)
                continue
            enriched = dict(record)
            enriched[field] = func(record)
            result.append(enriched)
        return result

    transform.__name__ = f"enrich_field({field!r})"
    return transform


def enrich_from(
    mapping: Dict[str, Callable[[Dict], Any]],
    *,
    overwrite: bool = True,
) -> Callable[[List[Dict]], List[Dict]]:
    """Enrich multiple fields at once using a mapping of field -> func.

    Args:
        mapping: Dict of field names to callables that derive the value.
        overwrite: If False, skip fields already present on a record.

    Returns:
        A step function that enriches a list of records.
    """
    def transform(records: List[Dict]) -> List[Dict]:
        result = []
        for record in records:
            enriched = dict(record)
            for field, func in mapping.items():
                if not overwrite and field in enriched:
                    continue
                enriched[field] = func(record)
            result.append(enriched)
        return result

    transform.__name__ = "enrich_from(mapping)"
    return transform


def enrich_constant(
    field: str,
    value: Any,
    *,
    overwrite: bool = True,
) -> Callable[[List[Dict]], List[Dict]]:
    """Set a constant value on a field for every record.

    Args:
        field: The field name to set.
        value: The constant value to assign.
        overwrite: If False, skip records that already have the field.

    Returns:
        A step function that sets the constant field.
    """
    return enrich_field(field, lambda _: value, overwrite=overwrite)
