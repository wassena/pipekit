"""Example: filling missing fields with coalesce before processing."""

from pipekit import Pipeline
from pipekit.coalesce import coalesce_field, coalesce_fields
from pipekit.tap import tap
from pipekit.io import save_json

# ---------------------------------------------------------------------------
# Sample data — some records have missing / empty fields
# ---------------------------------------------------------------------------
raw_records = [
    {"id": 1, "display_name": "Alice",  "username": "alice",  "score": 88},
    {"id": 2, "display_name": None,     "username": "bob",    "score": None},
    {"id": 3, "display_name": None,     "username": None,     "score": -1},
    {"id": 4, "display_name": "Diana",  "username": "diana",  "score": 0},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _log(label: str):
    def _inner(records):
        print(f"[{label}] {len(records)} record(s)")
        for r in records:
            print(f"  {r}")
        return records
    return _inner


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
pipeline = Pipeline(
    steps=[
        # Fill display_name from username, then fall back to "Guest"
        coalesce_field("display_name", "username", "Guest"),

        # Treat -1 and None as missing for score; default to 0
        coalesce_field("score", 0, null_values=[None, -1]),

        # Ensure both fields are definitely non-None (belt-and-braces)
        coalesce_fields(["display_name", "score"], default="N/A"),

        tap(_log("after coalesce")),
    ],
    name="coalesce-example",
)


if __name__ == "__main__":
    result = pipeline(raw_records)
    print("\nFinal output:")
    for rec in result:
        print(rec)
