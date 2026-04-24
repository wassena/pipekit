"""Example: reshape a sales dataset with pivot and melt."""

from pipekit.pipeline import Pipeline
from pipekit.pivot import melt_step, pivot_step
from pipekit.tap import tap

# ---------------------------------------------------------------------------
# Simulated source data — one row per (rep, quarter, metric)
# ---------------------------------------------------------------------------
RAW_DATA = [
    {"rep": "alice", "quarter": "Q1", "metric": "revenue", "amount": 120_000},
    {"rep": "alice", "quarter": "Q1", "metric": "deals",   "amount": 8},
    {"rep": "alice", "quarter": "Q2", "metric": "revenue", "amount": 95_000},
    {"rep": "alice", "quarter": "Q2", "metric": "deals",   "amount": 6},
    {"rep": "bob",   "quarter": "Q1", "metric": "revenue", "amount": 200_000},
    {"rep": "bob",   "quarter": "Q1", "metric": "deals",   "amount": 14},
    {"rep": "bob",   "quarter": "Q2", "metric": "revenue", "amount": 180_000},
    {"rep": "bob",   "quarter": "Q2", "metric": "deals",   "amount": 11},
]


def _log(label):
    def _inner(data):
        print(f"\n=== {label} ===")
        for row in data:
            print(" ", row)
        return data
    _inner.__name__ = label
    return _inner


# ---------------------------------------------------------------------------
# Pipeline 1: pivot metrics into columns, grouped by (rep, quarter)
# ---------------------------------------------------------------------------
# After pivot each row looks like:
#   {"rep": "alice", "quarter": "Q1", "deals": 8, "revenue": 120000}

wide_pipeline = Pipeline(
    [
        _log("raw long-form"),
        pivot_step(index="rep", column="metric", value="amount", agg=sum),
        _log("wide form (pivot)"),
    ],
    name="wide_pipeline",
)

# ---------------------------------------------------------------------------
# Pipeline 2: melt the wide form back to long form
# ---------------------------------------------------------------------------

long_pipeline = Pipeline(
    [
        melt_step(
            id_fields=["rep"],
            column_name="metric",
            value_name="amount",
        ),
        _log("long form again (melt)"),
    ],
    name="long_pipeline",
)


if __name__ == "__main__":
    wide_records = wide_pipeline(RAW_DATA)
    long_records = long_pipeline(wide_records)
    print(f"\nRound-trip record count: {len(long_records)} (expected {len(wide_records) * 2})")
