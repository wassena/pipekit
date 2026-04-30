# `pipekit.bucket` — Record Bucketing

Assign records into named buckets based on numeric thresholds or arbitrary
predicates, then optionally collect them into a grouped dictionary.

---

## `bucket_by_thresholds(field, thresholds, *, default, output_field)`

Assign a bucket label by comparing a numeric field against ordered upper bounds.
The **first** threshold whose bound the value is *strictly less than* wins.

```python
from pipekit.bucket import bucket_by_thresholds

step = bucket_by_thresholds(
    "score",
    [("low", 40), ("mid", 70), ("high", 100)],
    default="other",
)

records = [{"score": 20}, {"score": 55}, {"score": 85}, {"score": 100}]
print(step(records))
# [
#   {"score": 20, "bucket": "low"},
#   {"score": 55, "bucket": "mid"},
#   {"score": 85, "bucket": "high"},
#   {"score": 100, "bucket": "other"},   # 100 is not < 100
# ]
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `field` | — | Numeric field to evaluate |
| `thresholds` | — | `[(label, upper_bound), …]` in ascending order |
| `default` | `"other"` | Label when no threshold matches or value is `None` |
| `output_field` | `"bucket"` | Field written to each output record |

---

## `bucket_by_predicate(buckets, *, default, output_field)`

Assign a bucket label using arbitrary per-record predicate functions.
The **first** truthy predicate wins.

```python
from pipekit.bucket import bucket_by_predicate

step = bucket_by_predicate([
    ("vip",     lambda r: r["spend"] > 1000),
    ("regular", lambda r: r["spend"] > 100),
], default="new")

records = [{"spend": 1500}, {"spend": 200}, {"spend": 10}]
print(step(records))
# [{"spend": 1500, "bucket": "vip"}, {"spend": 200, "bucket": "regular"}, ...]
```

---

## `collect_buckets(output_field)`

Collapse a flat list of bucketed records into a `dict[label → list[record]]`.
Typically the **last** step in a bucket pipeline.

```python
from pipekit.pipeline import Pipeline
from pipekit.bucket import bucket_by_thresholds, collect_buckets

pipeline = Pipeline([
    bucket_by_thresholds("price", [("cheap", 10), ("mid", 50)], default="expensive"),
    collect_buckets(),
])

buckets = pipeline([{"price": 5}, {"price": 25}, {"price": 99}])
for label, items in buckets.items():
    print(label, items)
```

---

## Notes

- Input records are **never mutated**; each output record is a shallow copy.
- `bucket_by_thresholds` treats `None` field values as unmatched → `default`.
- Both helpers accept an `output_field` parameter so multiple bucketing steps
  can coexist in the same pipeline without collision.
