"""Example: using checkpoints to resume a multi-step pipeline."""

import time
from pipekit.pipeline import Pipeline
from pipekit.checkpoint import checkpoint, clear_checkpoints

RAW = [{"id": i, "value": i * 3} for i in range(20)]


@checkpoint("loaded", checkpoint_dir=".checkpoints/example")
def load(data):
    print("[load] running...")
    time.sleep(0.1)  # simulate I/O
    return data


@checkpoint("filtered", checkpoint_dir=".checkpoints/example")
def filter_positive(records):
    print("[filter] running...")
    return [r for r in records if r["value"] > 10]


@checkpoint("enriched", checkpoint_dir=".checkpoints/example")
def enrich(records):
    print("[enrich] running...")
    return [{**r, "label": f"item-{r['id']}"} for r in records]


def summarise(records):
    print(f"[summarise] {len(records)} records")
    for r in records[:3]:
        print(" ", r)
    return records


pipeline = Pipeline([load, filter_positive, enrich, summarise])

if __name__ == "__main__":
    import sys

    if "--clear" in sys.argv:
        removed = clear_checkpoints(".checkpoints/example")
        print(f"Cleared {removed} checkpoint(s).")
    else:
        print("=== First run (or resumed) ===")
        pipeline(RAW)
        print("\n=== Second run (all cached) ===")
        pipeline(RAW)
