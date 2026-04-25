"""Join utilities for combining two record sets by a shared key field."""

from typing import Any, Callable, Dict, List, Optional


def inner_join(
    left: List[Dict],
    right: List[Dict],
    on: str,
    suffixes: tuple = ("_left", "_right"),
) -> List[Dict]:
    """Return records where *on* key exists in both left and right."""
    right_index: Dict[Any, List[Dict]] = {}
    for rec in right:
        key = rec.get(on)
        right_index.setdefault(key, []).append(rec)

    result = []
    for left_rec in left:
        key = left_rec.get(on)
        for right_rec in right_index.get(key, []):
            merged = _merge(left_rec, right_rec, on, suffixes)
            result.append(merged)
    return result


def left_join(
    left: List[Dict],
    right: List[Dict],
    on: str,
    suffixes: tuple = ("_left", "_right"),
) -> List[Dict]:
    """Return all left records, enriched with matching right records (or None values)."""
    right_index: Dict[Any, List[Dict]] = {}
    for rec in right:
        key = rec.get(on)
        right_index.setdefault(key, []).append(rec)

    result = []
    for left_rec in left:
        key = left_rec.get(on)
        matches = right_index.get(key, [])
        if matches:
            for right_rec in matches:
                result.append(_merge(left_rec, right_rec, on, suffixes))
        else:
            null_right = {k: None for k in _right_keys(right, on)}
            result.append({**left_rec, **null_right})
    return result


def full_join(
    left: List[Dict],
    right: List[Dict],
    on: str,
    suffixes: tuple = ("_left", "_right"),
) -> List[Dict]:
    """Return all records from both left and right, with None for missing matches.

    Records that match on the *on* key are merged; unmatched left records have
    None for right-side fields, and unmatched right records have None for
    left-side fields.
    """
    result = left_join(left, right, on=on, suffixes=suffixes)

    left_keys = {rec.get(on) for rec in left}
    null_left = {k: None for k in _right_keys(left, on)}
    for right_rec in right:
        if right_rec.get(on) not in left_keys:
            result.append({**null_left, **right_rec})

    return result


def join_step(
    right: List[Dict],
    on: str,
    how: str = "inner",
    suffixes: tuple = ("_left", "_right"),
) -> Callable[[List[Dict]], List[Dict]]:
    """Return a pipeline-compatible step that joins incoming data with *right*."""
    if how not in ("inner", "left", "full"):
        raise ValueError(f"Unsupported join type: {how!r}. Use 'inner', 'left', or 'full'.")

    def transform(data: List[Dict]) -> List[Dict]:
        if how == "inner":
            return inner_join(data, right, on=on, suffixes=suffixes)
        if how == "full":
            return full_join(data, right, on=on, suffixes=suffixes)
        return left_join(data, right, on=on, suffixes=suffixes)

    transform.__name__ = f"join_step({how}, on={on!r})"
    return transform


# ── helpers ──────────────────────────────────────────────────────────────────

def _right_keys(right: List[Dict], exclude: str) -> List[str]:
    keys: List[str] = []
    for rec in right:
        for k in rec:
            if k != exclude and k not in keys:
                keys.append(k)
    return keys


def _merge(
    left: Dict,
    right: Dict,
    on: str,
    suffixes: tuple,
) -> Dict:
    shared = {k for k in left if k in right and k != on}
    merged: Dict = {on: left[on]}
    for k, v in left.items():
        if k == on:
            continue
        merged[k + suffixes[0] if k in shared else k] = v
    for k, v in right.items():
        if k == on:
            continue
        merged[k + suffixes[1] if k in shared else k] = v
    return merged
