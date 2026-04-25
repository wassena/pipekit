"""Example: load a CSV, cast fields to correct types, then save as JSON."""

from __future__ import annotations

import json
import pathlib
import tempfile

from pipekit.pipeline import Pipeline, Step
from pipekit.typecast import CastError, cast_field, cast_fields

# ---------------------------------------------------------------------------
# Inline data (avoids needing a real CSV file for the demo)
# ---------------------------------------------------------------------------

RAW_RECORDS = [
    {"name": "Alice", "age": "30", "score": "8.5",  "active": "1"},
    {"name": "Bob",   "age": "25", "score": "6.0",  "active": "0"},
    {"name": "Carol", "age": "35", "score": "9.25", "active": "1"},
    {"name": "Dave",  "age": "28", "score": "bad",  "active": "1"},  # bad score
]


# ---------------------------------------------------------------------------
# Helper steps
# ---------------------------------------------------------------------------

def _log(label: str):
    def _inner(records):
        print(f"[{label}] {len(records)} record(s)")
        return records
    _inner.__name__ = f"log({label})"
    return _inner


def drop_cast_errors(field: str, target: type):
    """Try to cast *field*; silently drop records that fail."""
    def _inner(records):
        out = []
        for rec in records:
            try:
                out.append(cast_field(field, target, strict=True)([rec])[0])
            except CastError as exc:
                print(f"  [drop] {rec.get('name', '?')!r}: {exc}")
        return out
    _inner.__name__ = f"drop_cast_errors({field!r})"
    return _inner


def flag_active(records):
    """Convert the 'active' field from int to bool."""
    return [{**r, "active": bool(r["active"])} for r in records]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def build_pipeline():
    return Pipeline([
        Step(lambda _: RAW_RECORDS, name="source"),
        _log("after source"),
        cast_field("age", int),
        drop_cast_errors("score", float),
        cast_field("active", int),
        flag_active,
        _log("after casting"),
    ])


if __name__ == "__main__":
    pipeline = build_pipeline()
    result = pipeline(None)

    print("\nFinal records:")
    for rec in result:
        print(" ", rec)

    # Persist to a temp file to show end-to-end flow
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as fh:
        json.dump(result, fh, indent=2)
        print(f"\nSaved to {fh.name}")
