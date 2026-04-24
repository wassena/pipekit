"""Aggregation utilities for pipekit pipelines."""

from typing import Any, Callable, Dict, Iterable, List, Optional


def group_by(
    key: str,
    records: List[Dict[str, Any]],
) -> Dict[Any, List[Dict[str, Any]]]:
    """Group a list of records by the value of a given field.

    Args:
        key: The field name to group by.
        records: The list of dicts to group.

    Returns:
        A dict mapping each unique key value to the list of matching records.
    """
    groups: Dict[Any, List[Dict[str, Any]]] = {}
    for record in records:
        group_key = record[key]
        groups.setdefault(group_key, []).append(record)
    return groups


def aggregate(
    key: str,
    agg_field: str,
    func: Callable[[List[Any]], Any],
    output_field: Optional[str] = None,
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Create a step that groups records by *key* and applies *func* to
    the values of *agg_field* within each group.

    Args:
        key: Field to group by.
        agg_field: Field whose values are aggregated.
        func: Aggregation function (e.g. sum, len, max).
        output_field: Name for the result field. Defaults to agg_field.

    Returns:
        A callable step that transforms a list of records into aggregated records.
    """
    result_field = output_field or agg_field

    def transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups = group_by(key, records)
        result = []
        for group_key, group_records in groups.items():
            values = [r[agg_field] for r in group_records if agg_field in r]
            result.append({key: group_key, result_field: func(values)})
        return result

    transform.__name__ = f"aggregate({key}, {agg_field})"
    return transform


def count_by(
    key: str,
    output_field: str = "count",
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Create a step that counts records in each group defined by *key*.

    Args:
        key: Field to group by.
        output_field: Name for the count field. Defaults to "count".

    Returns:
        A callable step producing one record per group with the count.
    """
    return aggregate(key, key, len, output_field=output_field)
