"""Example: use sample_step to prototype on a small slice of data."""

import json
import pathlib
import tempfile

from pipekit import Pipeline, Step
from pipekit.sample import sample_step, reservoir_sample
from pipekit.transforms import map_field

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dataset(n: int = 200):
    """Generate a synthetic list of records for demonstration."""
    return [
        {"id": i, "value": i * 1.5, "label": "even" if i % 2 == 0 else "odd"}
        for i in range(n)
    ]


def _log(label: str):
    def _inner(data):
        print(f"[{label}] {len(data)} records — first: {data[0] if data else 'n/a'}")
        return data
    _inner.__name__ = f"log_{label}"
    return _inner


# ---------------------------------------------------------------------------
# Transform steps
# ---------------------------------------------------------------------------

def scale_value(data):
    """Multiply 'value' by 10."""
    return [{**r, "value": r["value"] * 10} for r in data]


# ---------------------------------------------------------------------------
# Pipeline A — fraction-based sampling
# ---------------------------------------------------------------------------

fraction_pipeline = Pipeline(
    [
        Step(lambda _: _make_dataset(200), name="generate"),
        _log("raw"),
        sample_step(fraction=0.1, seed=42),
        _log("sampled"),
        scale_value,
        _log("scaled"),
    ]
)

# ---------------------------------------------------------------------------
# Pipeline B — fixed-n sampling
# ---------------------------------------------------------------------------

fixed_pipeline = Pipeline(
    [
        Step(lambda _: _make_dataset(500), name="generate"),
        _log("raw"),
        sample_step(n=20, seed=0),
        _log("sampled"),
    ]
)

# ---------------------------------------------------------------------------
# Standalone reservoir example
# ---------------------------------------------------------------------------

def reservoir_example():
    big_data = list(range(100_000))
    subset = reservoir_sample(big_data, k=10, seed=99)
    print(f"Reservoir sample (k=10): {subset}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Fraction-based pipeline ===")
    fraction_pipeline(None)

    print("\n=== Fixed-n pipeline ===")
    fixed_pipeline(None)

    print("\n=== Reservoir sample ===")
    reservoir_example()
