"""Example: normalize numeric fields before feeding records into a model."""

from pipekit import Pipeline
from pipekit.normalize import clamp_field, normalize_field, round_field
from pipekit.io import load_csv, save_json
from pipekit.tap import tap
from pipekit.validators import validate_fields


# ---------------------------------------------------------------------------
# Side-effect helpers
# ---------------------------------------------------------------------------

def _log(label):
    def _inner(records):
        print(f"[{label}] {len(records)} records")
        return records
    return _inner


def _preview(records):
    for r in records[:3]:
        print(" ", r)
    return records


# ---------------------------------------------------------------------------
# Pipeline definition
# ---------------------------------------------------------------------------

def build_pipeline(input_csv: str, output_json: str):
    """Construct and run a normalization pipeline."""

    raw = load_csv(input_csv)

    # Cast string values coming from CSV to float
    def _cast(records):
        return [
            {**r, "age": float(r["age"]), "salary": float(r["salary"])}
            for r in records
        ]

    pipeline = Pipeline(
        [
            tap(_log("loaded")),
            validate_fields(["name", "age", "salary"]),
            _cast,
            tap(_log("cast")),
            clamp_field("age", 18, 90),
            clamp_field("salary", 0, 500_000),
            normalize_field("age"),
            normalize_field("salary"),
            round_field("age", decimals=4),
            round_field("salary", decimals=4),
            tap(_log("normalized")),
            tap(_preview),
        ]
    )

    result = pipeline(raw)
    save_json(result, output_json)
    print(f"Saved {len(result)} records to {output_json}")
    return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: normalize_pipeline.py <input.csv> <output.json>")
        sys.exit(1)

    build_pipeline(sys.argv[1], sys.argv[2])
