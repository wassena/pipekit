"""Scoring utilities for pipekit pipelines.

Provides functions to assign numeric scores to records based on weighted
field values or custom scoring functions.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


def score_field(
    weights: Dict[str, float],
    output_field: str = "score",
    missing: float = 0.0,
    normalise: bool = False,
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Compute a weighted sum score from named fields.

    Args:
        weights: Mapping of field name to its numeric weight.
        output_field: Name of the field to store the computed score.
        missing: Value to use when a field is absent or None.
        normalise: If True, divide the score by the sum of absolute weights.

    Returns:
        A step function that adds *output_field* to every record.
    """
    total_weight = sum(abs(w) for w in weights.values()) or 1.0

    def transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for rec in records:
            s = sum(
                weights[field] * (rec[field] if rec.get(field) is not None else missing)
                for field in weights
            )
            if normalise:
                s = s / total_weight
            out.append({**rec, output_field: s})
        return out

    return transform


def score_by(
    func: Callable[[Dict[str, Any]], float],
    output_field: str = "score",
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Assign a score to each record using an arbitrary callable.

    Args:
        func: Callable that receives a record dict and returns a float.
        output_field: Name of the field to store the result.

    Returns:
        A step function that adds *output_field* to every record.
    """
    def transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [{**rec, output_field: func(rec)} for rec in records]

    return transform


def rank_by(
    field: str,
    output_field: str = "rank",
    ascending: bool = False,
    start: int = 1,
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Add a rank field based on the value of *field*.

    Records with the same value receive the same rank (dense ranking).

    Args:
        field: The field whose values determine the ranking.
        output_field: Name of the field to store the rank.
        ascending: If True, lower values get lower (better) ranks.
        start: Starting rank number (default 1).

    Returns:
        A step function that adds *output_field* to every record.
    """
    def transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sorted_vals = sorted(
            {rec.get(field) for rec in records if rec.get(field) is not None},
            reverse=not ascending,
        )
        rank_map = {v: i + start for i, v in enumerate(sorted_vals)}
        return [{**rec, output_field: rank_map.get(rec.get(field))} for rec in records]

    return transform
