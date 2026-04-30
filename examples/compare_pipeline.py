"""Example: using compare_field and compare_fields in a pipeline."""

from pipekit import Pipeline
from pipekit.compare import compare_field, compare_fields
from pipekit.select import select_fields
from pipekit.tap import tap

# ---------------------------------------------------------------------------
# Sample dataset – sales records
# ---------------------------------------------------------------------------
records = [
    {"rep": "Alice", "sales": 120, "target": 100, "region": "north"},
    {"rep": "Bob",   "sales": 80,  "target": 100, "region": "south"},
    {"rep": "Carol", "sales": 100, "target": 100, "region": "north"},
    {"rep": "Dave",  "sales": 55,  "target": 80,  "region": "east"},
    {"rep": "Eve",   "sales": 0,   "target": 50,  "region": "west"},
]


def _log(label):
    def _inner(data):
        print(f"\n--- {label} ---")
        for r in data:
            print(r)
        return data
    return _inner


# ---------------------------------------------------------------------------
# Build pipeline
# ---------------------------------------------------------------------------
pipeline = Pipeline(
    steps=[
        # Flag reps who met or exceeded their individual target
        compare_fields("sales", "ge", "target", output_field="hit_target"),
        # Flag reps who exceeded the global quota of 100
        compare_field("sales", "gt", 100, output_field="above_quota"),
        # Flag reps in the northern region
        compare_field("region", "eq", "north", output_field="is_north"),
        # Flag reps with zero sales
        compare_field("sales", "eq", 0, output_field="no_sales"),
        tap(_log("Enriched records")),
        # Keep only the summary columns
        select_fields(["rep", "sales", "target", "hit_target", "above_quota", "is_north", "no_sales"]),
        tap(_log("Final output")),
    ]
)

if __name__ == "__main__":
    result = pipeline(records)
    print("\nReps who hit their target:")
    for r in result:
        if r["hit_target"]:
            print(" ", r["rep"])

    print("\nReps above global quota (100):")
    for r in result:
        if r["above_quota"]:
            print(" ", r["rep"])
