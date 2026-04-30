"""Example: segment customers by spend tier using bucket utilities."""

from pipekit.pipeline import Pipeline
from pipekit.bucket import bucket_by_thresholds, bucket_by_predicate, collect_buckets
from pipekit.tap import tap
from pipekit.enrich import enrich_field

# ---------------------------------------------------------------------------
# Sample dataset
# ---------------------------------------------------------------------------
customers = [
    {"id": 1, "name": "Alice",   "spend": 1200, "orders": 15},
    {"id": 2, "name": "Bob",     "spend": 340,  "orders": 4},
    {"id": 3, "name": "Carol",   "spend": 85,   "orders": 2},
    {"id": 4, "name": "Dave",    "spend": 5,    "orders": 1},
    {"id": 5, "name": "Eve",     "spend": 720,  "orders": 9},
    {"id": 6, "name": "Frank",   "spend": None, "orders": 0},
]


def _log(label):
    def _inner(data):
        if isinstance(data, list):
            print(f"[{label}] {len(data)} records")
        else:
            print(f"[{label}] buckets: {list(data.keys())}")
        return data
    return _inner


# ---------------------------------------------------------------------------
# Pipeline 1 — threshold bucketing by spend
# ---------------------------------------------------------------------------
spend_pipeline = Pipeline([
    tap(_log("input")),
    bucket_by_thresholds(
        "spend",
        [("low", 100), ("mid", 500), ("high", 1000)],
        default="premium",
        output_field="spend_tier",
    ),
    tap(_log("after threshold bucket")),
    collect_buckets(output_field="spend_tier"),
    _log("collected"),
])

print("=" * 50)
print("Spend-tier pipeline")
print("=" * 50)
tiers = spend_pipeline(customers)
for tier, members in tiers.items():
    names = [c["name"] for c in members]
    print(f"  {tier:10s}: {names}")


# ---------------------------------------------------------------------------
# Pipeline 2 — predicate bucketing (VIP logic)
# ---------------------------------------------------------------------------
vip_pipeline = Pipeline([
    bucket_by_predicate(
        [
            ("vip",    lambda r: (r.get("spend") or 0) > 1000 and r["orders"] >= 10),
            ("loyal",  lambda r: r["orders"] >= 5),
            ("active", lambda r: r["orders"] >= 1),
        ],
        default="inactive",
        output_field="segment",
    ),
    collect_buckets(output_field="segment"),
])

print()
print("=" * 50)
print("VIP-segment pipeline")
print("=" * 50)
segments = vip_pipeline(customers)
for seg, members in segments.items():
    names = [c["name"] for c in members]
    print(f"  {seg:10s}: {names}")
