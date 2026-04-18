"""Example: batch-processing a large record list through a pipekit Pipeline."""

from pipekit import Pipeline, Step
from pipekit.batch import process_batches
from pipekit.transforms import map_field, rename_field
from pipekit.validators import validate_fields, validate_type

# --- sample data ---
raw_records = [
    {"first_name": "Alice", "age": "30"},
    {"first_name": "Bob", "age": "25"},
    {"first_name": "Carol", "age": "40"},
    {"first_name": "Dave", "age": "22"},
    {"first_name": "Eve", "age": "35"},
]

# --- per-record pipeline ---
record_pipeline = Pipeline(
    [
        Step(validate_fields(["first_name", "age"]), name="validate_fields"),
        Step(map_field("age", int), name="parse_age"),
        Step(validate_type("age", int), name="validate_age_type"),
        Step(rename_field("first_name", "name"), name="rename"),
    ]
)


def transform_batch(batch):
    """Apply the per-record pipeline to every record in the batch."""
    return [record_pipeline(record) for record in batch]


skipped = []


def on_error(exc, chunk):
    print(f"[warn] skipping batch of {len(chunk)} due to: {exc}")
    skipped.extend(chunk)


results = process_batches(
    raw_records,
    transform_batch,
    size=2,
    on_error=on_error,
)

print("Processed records:")
for r in results:
    print(" ", r)

if skipped:
    print(f"Skipped {len(skipped)} records.")
