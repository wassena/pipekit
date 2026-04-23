"""Example: enforce a schema while loading CSV data."""

from pipekit import Pipeline
from pipekit.io import load_csv, save_json
from pipekit.schema import schema_step, SchemaError
from pipekit.tap import tap

# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------

RECORD_SCHEMA = {
    "id": int,
    "name": str,
    "score": float,
    "active": int,   # 0 / 1 flag stored as int in CSV
}

# ---------------------------------------------------------------------------
# Side-effect helpers
# ---------------------------------------------------------------------------

def _log(records):
    print(f"[schema_pipeline] {len(records)} records loaded")


def _warn_on_empty(records):
    if not records:
        print("[schema_pipeline] WARNING: no records passed schema validation")


# ---------------------------------------------------------------------------
# Graceful error filter
# ---------------------------------------------------------------------------

def drop_invalid(records: list) -> list:
    """Filter out records that fail schema validation instead of aborting."""
    validate = schema_step(RECORD_SCHEMA, coerce=True)
    good = []
    for record in records:
        try:
            good.extend(validate([record]))
        except SchemaError as exc:
            print(f"[schema_pipeline] Dropping record {record}: {exc}")
    return good


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

pipeline = Pipeline([
    tap(_log),
    drop_invalid,
    tap(_warn_on_empty),
])


if __name__ == "__main__":
    import tempfile, os, json

    sample = [
        {"id": "1", "name": "Alice", "score": "9.5",  "active": "1"},
        {"id": "2", "name": "Bob",   "score": "bad",   "active": "0"},
        {"id": "3", "name": "Carol", "score": "7.2",  "active": "1"},
    ]

    result = pipeline(sample)
    print("Valid records:", json.dumps(result, indent=2))
