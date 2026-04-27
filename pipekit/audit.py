"""Audit trail utilities for tracking record-level changes through a pipeline."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional


def audit_field(
    field: str,
    *,
    label: Optional[str] = None,
    include_timestamp: bool = False,
) -> Callable[[List[Dict]], List[Dict]]:
    """Record the value of *field* in an ``_audit`` log attached to each record.

    Args:
        field: The field whose value should be captured.
        label: Human-readable label stored in the audit entry.  Defaults to
               the field name.
        include_timestamp: When *True*, attach a UTC epoch timestamp to each
                           audit entry.
    """
    _label = label or field

    def transform(records: List[Dict]) -> List[Dict]:
        out = []
        for rec in records:
            rec = dict(rec)
            entry: Dict[str, Any] = {"field": _label, "value": rec.get(field)}
            if include_timestamp:
                entry["ts"] = time.time()
            audit = list(rec.get("_audit", []))
            audit.append(entry)
            rec["_audit"] = audit
            out.append(rec)
        return out

    return transform


def audit_step(
    label: str,
    *,
    fields: Optional[List[str]] = None,
    include_timestamp: bool = False,
) -> Callable[[List[Dict]], List[Dict]]:
    """Snapshot a whole record (or a subset of *fields*) under *label*.

    The snapshot is appended to ``_audit`` as a dict with keys
    ``{"step": label, "snapshot": {...}[, "ts": ...]}``.
    """

    def transform(records: List[Dict]) -> List[Dict]:
        out = []
        for rec in records:
            rec = dict(rec)
            snapshot = (
                {k: rec[k] for k in fields if k in rec}
                if fields
                else {k: v for k, v in rec.items() if k != "_audit"}
            )
            entry: Dict[str, Any] = {"step": label, "snapshot": snapshot}
            if include_timestamp:
                entry["ts"] = time.time()
            audit = list(rec.get("_audit", []))
            audit.append(entry)
            rec["_audit"] = audit
            out.append(rec)
        return out

    return transform


def strip_audit(records: List[Dict]) -> List[Dict]:
    """Remove the ``_audit`` key from every record before final output."""
    return [{k: v for k, v in rec.items() if k != "_audit"} for rec in records]


def get_audit_log(record: Dict) -> List[Dict]:
    """Return the audit log attached to *record*, or an empty list."""
    return list(record.get("_audit", []))
