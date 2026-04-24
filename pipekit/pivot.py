"""Pivot and unpivot (melt) utilities for record-oriented data."""

from typing import Any, Callable, Dict, List, Optional


def pivot(
    records: List[Dict[str, Any]],
    index: str,
    column: str,
    value: str,
    agg: Callable = None,
) -> List[Dict[str, Any]]:
    """Pivot records so unique values of *column* become new fields.

    Args:
        records:  Input list of dicts.
        index:    Field whose value identifies each output row.
        column:   Field whose values become new column names.
        value:    Field whose values fill the new columns.
        agg:      Optional aggregation callable (e.g. sum, max) used when
                  multiple source rows map to the same (index, column) cell.
                  Defaults to keeping the last value seen.

    Returns:
        List of dicts, one per unique *index* value.
    """
    if not records:
        return []

    # Collect raw cells: {index_val: {col_val: [values]}}
    cells: Dict[Any, Dict[Any, List[Any]]] = {}
    index_order: List[Any] = []

    for rec in records:
        idx = rec[index]
        col = rec[column]
        val = rec[value]
        if idx not in cells:
            cells[idx] = {}
            index_order.append(idx)
        cells[idx].setdefault(col, []).append(val)

    all_columns = sorted({rec[column] for rec in records}, key=str)

    result = []
    for idx in index_order:
        row: Dict[str, Any] = {index: idx}
        for col in all_columns:
            vals = cells[idx].get(col, [])
            if not vals:
                row[col] = None
            elif agg is not None:
                row[col] = agg(vals)
            else:
                row[col] = vals[-1]
        result.append(row)

    return result


def melt(
    records: List[Dict[str, Any]],
    id_fields: List[str],
    value_fields: Optional[List[str]] = None,
    column_name: str = "variable",
    value_name: str = "value",
) -> List[Dict[str, Any]]:
    """Unpivot wide records into long form.

    Args:
        records:      Input list of dicts.
        id_fields:    Fields to keep as-is in every output row.
        value_fields: Fields to melt; defaults to all non-id fields.
        column_name:  Name for the new 'variable' field.
        value_name:   Name for the new 'value' field.

    Returns:
        List of dicts in long form.
    """
    if not records:
        return []

    result = []
    for rec in records:
        if value_fields is None:
            cols = [k for k in rec if k not in id_fields]
        else:
            cols = value_fields

        id_part = {f: rec[f] for f in id_fields if f in rec}
        for col in cols:
            row = dict(id_part)
            row[column_name] = col
            row[value_name] = rec.get(col)
            result.append(row)

    return result


def pivot_step(
    index: str,
    column: str,
    value: str,
    agg: Callable = None,
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Return a pipeline-compatible step that calls :func:`pivot`."""
    def transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return pivot(records, index=index, column=column, value=value, agg=agg)
    transform.__name__ = "pivot_step"
    return transform


def melt_step(
    id_fields: List[str],
    value_fields: Optional[List[str]] = None,
    column_name: str = "variable",
    value_name: str = "value",
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Return a pipeline-compatible step that calls :func:`melt`."""
    def transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return melt(
            records,
            id_fields=id_fields,
            value_fields=value_fields,
            column_name=column_name,
            value_name=value_name,
        )
    transform.__name__ = "melt_step"
    return transform
