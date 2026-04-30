"""bucket.py — Assign records into named buckets based on thresholds or predicates."""

from typing import Any, Callable, Dict, List, Optional, Tuple


def bucket_by_thresholds(
    field: str,
    thresholds: List[Tuple[str, float]],
    *,
    default: str = "other",
    output_field: str = "bucket",
) -> Callable[[List[Dict]], List[Dict]]:
    """Assign each record a bucket label based on ordered numeric thresholds.

    Thresholds are evaluated in order; the first one whose bound the field
    value is *less than* wins.  Records that exceed every bound fall into
    *default*.

    Args:
        field: Numeric field to evaluate.
        thresholds: Ordered list of (label, upper_bound) pairs.
        default: Label used when no threshold matches.
        output_field: Destination field for the bucket label.

    Example::

        step = bucket_by_thresholds("score", [("low", 40), ("mid", 70), ("high", 100)])
        step([{"score": 35}, {"score": 65}, {"score": 95}])
        # [{"score": 35, "bucket": "low"}, ...]
    """

    def transform(records: List[Dict]) -> List[Dict]:
        out = []
        for rec in records:
            value = rec.get(field)
            label = default
            if value is not None:
                for lbl, bound in thresholds:
                    if value < bound:
                        label = lbl
                        break
            out.append({**rec, output_field: label})
        return out

    return transform


def bucket_by_predicate(
    buckets: List[Tuple[str, Callable[[Dict], bool]]],
    *,
    default: str = "other",
    output_field: str = "bucket",
) -> Callable[[List[Dict]], List[Dict]]:
    """Assign each record a bucket label using arbitrary predicate functions.

    Predicates are evaluated in order; the first truthy one wins.

    Args:
        buckets: Ordered list of (label, predicate) pairs.
        default: Label used when no predicate matches.
        output_field: Destination field for the bucket label.
    """

    def transform(records: List[Dict]) -> List[Dict]:
        out = []
        for rec in records:
            label = default
            for lbl, pred in buckets:
                if pred(rec):
                    label = lbl
                    break
            out.append({**rec, output_field: label})
        return out

    return transform


def collect_buckets(
    output_field: str = "bucket",
) -> Callable[[List[Dict]], Dict[str, List[Dict]]]:
    """Group records by their bucket label into a dict of lists.

    Returns a *dict* rather than a list, so it is typically the final step
    in a bucket-oriented pipeline.
    """

    def transform(records: List[Dict]) -> Dict[str, List[Dict]]:
        result: Dict[str, List[Dict]] = {}
        for rec in records:
            key = rec.get(output_field, "other")
            result.setdefault(key, []).append(rec)
        return result

    return transform
