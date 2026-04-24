"""diff.py — utilities for comparing records between pipeline stages."""

from typing import Any, Callable, Iterable, Iterator


def diff_records(
    before: Iterable[dict],
    after: Iterable[dict],
    key: str,
) -> dict[str, list[dict]]:
    """Compare two sequences of records by a unique key field.

    Returns a dict with three keys:
      - 'added':   records present in *after* but not *before*
      - 'removed': records present in *before* but not *after*
      - 'changed': records whose key exists in both but whose content differs

    Args:
        before: Original sequence of records.
        after:  Transformed sequence of records.
        key:    Field name used to match records across sequences.

    Raises:
        KeyError: If any record is missing the key field.
    """
    before_map = {r[key]: r for r in before}
    after_map = {r[key]: r for r in after}

    before_keys = set(before_map)
    after_keys = set(after_map)

    added = [after_map[k] for k in after_keys - before_keys]
    removed = [before_map[k] for k in before_keys - after_keys]
    changed = [
        {"before": before_map[k], "after": after_map[k]}
        for k in before_keys & after_keys
        if before_map[k] != after_map[k]
    ]

    return {"added": added, "removed": removed, "changed": changed}


def field_diff(record_a: dict, record_b: dict) -> dict[str, dict[str, Any]]:
    """Return a field-level diff between two individual records.

    Each entry in the result maps a field name to a dict with
    ``'before'`` and ``'after'`` values.  Only differing fields
    (or fields present in only one record) are included.

    Args:
        record_a: The original record.
        record_b: The updated record.
    """
    all_keys = set(record_a) | set(record_b)
    result: dict[str, dict[str, Any]] = {}
    _missing = object()
    for k in all_keys:
        a_val = record_a.get(k, _missing)
        b_val = record_b.get(k, _missing)
        if a_val != b_val:
            result[k] = {
                "before": None if a_val is _missing else a_val,
                "after": None if b_val is _missing else b_val,
            }
    return result


def diff_step(
    key: str,
    on_diff: Callable[[dict], None] | None = None,
) -> Callable[[list[dict]], list[dict]]:
    """Return a pipeline step that emits a diff report as a side-effect.

    The step receives a dict with ``'before'`` and ``'after'`` lists and
    passes ``'after'`` downstream unchanged.  If *on_diff* is provided it
    is called with the diff report produced by :func:`diff_records`.

    Args:
        key:     Field used to match records (passed to :func:`diff_records`).
        on_diff: Optional callback that receives the diff report dict.
    """
    def step(data: dict) -> list[dict]:
        before = data.get("before", [])
        after = data.get("after", [])
        report = diff_records(before, after, key=key)
        if on_diff is not None:
            on_diff(report)
        return after

    step.__name__ = f"diff_step(key={key!r})"
    return step
