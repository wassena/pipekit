"""Example: using cached_step to avoid recomputing expensive transforms."""

import time

from pipekit.cache import cached_step, clear_all_cache
from pipekit.pipeline import Pipeline, Step

CACHE_DIR = ".pipekit_cache_example"


@cached_step(cache_dir=CACHE_DIR)
def load_and_clean(records):
    """Simulate an expensive cleaning step."""
    time.sleep(0.05)  # pretend this is slow
    return [
        {k: v.strip() if isinstance(v, str) else v for k, v in r.items()}
        for r in records
    ]


@cached_step(cache_dir=CACHE_DIR)
def enrich(records):
    """Add a computed field to each record."""
    return [{**r, "score": len(r.get("name", ""))} for r in records]


def summarise(records):
    total = sum(r["score"] for r in records)
    print(f"Total score: {total} across {len(records)} records")
    return records


if __name__ == "__main__":
    raw_data = [
        {"name": "  Alice ", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": " Charlie ", "age": 35},
    ]

    pipeline = Pipeline(
        Step(load_and_clean),
        Step(enrich),
        Step(summarise),
    )

    print("First run (may compute):")
    start = time.time()
    pipeline(raw_data)
    print(f"  Elapsed: {time.time() - start:.3f}s")

    print("Second run (from cache):")
    start = time.time()
    pipeline(raw_data)
    print(f"  Elapsed: {time.time() - start:.3f}s")

    clear_all_cache(CACHE_DIR)
    print("Cache cleared.")
